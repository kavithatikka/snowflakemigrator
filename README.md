# snowflakemigrator
Below is a small, practical Python project that migrates data from any SQLAlchemy-compatible RDBMS (MySQL, Postgres, SQL Server, Oracle, SQLite, etc.) into Snowflake. It reads source data in chunks and uses Snowflake's write_pandas helper to efficiently load DataFrames into Snowflake.


# RDBMS -> Snowflake Migrator

Lightweight Python tool to migrate tables (or arbitrary queries) from any SQLAlchemy-compatible RDBMS into Snowflake.

Features
- Works with any RDBMS supported by SQLAlchemy (Postgres, MySQL, SQL Server, Oracle, SQLite, ...)
- Stream source data in configurable chunks to limit memory use
- Uses Snowflake's `write_pandas` for efficient upload and automatic table creation
- Supports YAML config or CLI args

Quick start

1. Create a Python virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Create a config YAML (see `example_config.yaml`) or pass connection strings via CLI.

3. Run the migration:

```bash
# using config file
python -m rdbms_to_snowflake.cli migrate --config example_config.yaml

# or via CLI parameters
python -m rdbms_to_snowflake.cli migrate \
  --source-url "postgresql+psycopg2://user:pass@host:5432/db" \
  --source-query "SELECT * FROM public.my_table" \
  --sf-user YOUR_USER --sf-password YOUR_PASSWORD --sf-account YOUR_ACCOUNT \
  --sf-database YOUR_DB --sf-schema PUBLIC --sf-warehouse YOUR_WH \
  --table DEST_TABLE --chunk-size 50000 --mode append
```

Notes and caveats
- The tool uses pandas to read source data and Snowflake's `write_pandas` to load it. For very large datasets you may prefer a staged bulk-copy approach (PUT + COPY INTO) — this tool aims for simplicity and broad compatibility.
- Column types are inferred; for complex type control, create the target table in Snowflake first with desired types and use `--mode append`.
- Credentials can be passed on CLI or a YAML config. Avoid putting secrets into source control.

Files included
- `src/rdbms_to_snowflake/cli.py` — command line interface
- `src/rdbms_to_snowflake/loader.py` — migration logic
- `src/rdbms_to_snowflake/config.py` — YAML config loader
- `example_config.yaml` — example config
- `requirements.txt` — python dependencies
- `.gitignore`

License
- MIT
