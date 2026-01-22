import logging
from typing import Dict, Optional, Iterator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from tqdm import tqdm

logger = logging.getLogger(__name__)


def create_source_engine(source_url: str) -> Engine:
    """
    Create a SQLAlchemy engine for the source database.
    Example source_url: postgresql+psycopg2://user:pass@host:5432/dbname
    """
    return create_engine(source_url, pool_pre_ping=True)


def connect_snowflake(sf_cfg: Dict[str, str]):
    """
    Create a snowflake-connector-python connection from config dict.
    Expected keys: user, password, account, warehouse, database, schema, role (optional)
    """
    conn_args = {
        "user": sf_cfg["user"],
        "password": sf_cfg["password"],
        "account": sf_cfg["account"],
        "warehouse": sf_cfg.get("warehouse"),
        "database": sf_cfg.get("database"),
        "schema": sf_cfg.get("schema"),
    }
    if sf_cfg.get("role"):
        conn_args["role"] = sf_cfg.get("role")
    # Remove None values
    conn_args = {k: v for k, v in conn_args.items() if v is not None}
    return snowflake.connector.connect(**conn_args)


def iter_source_chunks(
    engine: Engine,
    query: Optional[str] = None,
    table: Optional[str] = None,
    chunk_size: int = 50000,
):
    """
    Yield DataFrame chunks from source. Either query or table must be provided.
    """
    if query:
        # Use read_sql_query with chunksize generator
        for chunk in pd.read_sql_query(sql=text(query), con=engine.connect(), chunksize=chunk_size):
            yield chunk
    elif table:
        for chunk in pd.read_sql_table(table_name=table, con=engine.connect(), chunksize=chunk_size):
            yield chunk
    else:
        raise ValueError("Either query or table must be provided")


def migrate_table(
    source_url: str,
    sf_cfg: Dict[str, str],
    dest_table: str,
    source_query: Optional[str] = None,
    source_table: Optional[str] = None,
    chunk_size: int = 50000,
    mode: str = "append",
):
    """
    Migrate data from source to Snowflake.

    mode: 'append' or 'replace'
    """
    if mode not in ("append", "replace"):
        raise ValueError("mode must be 'append' or 'replace'")

    src_engine = create_source_engine(source_url)
    sf_conn = connect_snowflake(sf_cfg)

    # If replace mode, drop target table first
    if mode == "replace":
        cur = sf_conn.cursor()
        try:
            drop_sql = f'DROP TABLE IF EXISTS {sf_cfg.get("database")}.{sf_cfg.get("schema")}.{dest_table}'
            logger.info("Dropping target table (replace mode): %s", drop_sql)
            cur.execute(drop_sql)
        finally:
            cur.close()

    total_rows = 0
    nchunks = 0
    # iterate and push each chunk using write_pandas
    for df in iter_source_chunks(src_engine, query=source_query, table=source_table, chunk_size=chunk_size):
        if df.empty:
            continue
        # Normalize column names to strings
        df.columns = [str(c) for c in df.columns]
        logger.info("Uploading chunk with %d rows to %s", len(df), dest_table)
        success, n_chunks_uploaded, nrows, _ = write_pandas(
            conn=sf_conn,
            df=df,
            table_name=dest_table,
            database=sf_cfg.get("database"),
            schema=sf_cfg.get("schema"),
            chunk_size=min(len(df), chunk_size),
        )
        if not success:
            raise RuntimeError("write_pandas failed to upload chunk")
        total_rows += nrows
        nchunks += n_chunks_uploaded

    logger.info("Migration finished. total_rows=%s total_chunks=%s", total_rows, nchunks)
    sf_conn.close()
    return {"rows": total_rows, "chunks": nchunks}
