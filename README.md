# snowflakemigrator
Below is a small, practical Python project that migrates data from any SQLAlchemy-compatible RDBMS (MySQL, Postgres, SQL Server, Oracle, SQLite, etc.) into Snowflake. It reads source data in chunks and uses Snowflake's write_pandas helper to efficiently load DataFrames into Snowflake.
