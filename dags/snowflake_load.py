from airflow.sdk import task, dag
from datetime import datetime
import logging
import os

logger=logging.getLogger(__name__)

S3_BUCKET=os.getenv("S3_BUCKET", "finance-etl-bucket")
SNOWFLAKE_CONN="snowflake_default"
SNOWFLAKE_TABLE="fact_ohlcv"
SNOWFLAKE_SCHEMA="finance"
SNOWFLAKE_DB="FINANCE_DW"

@task
def load_parquet_to_snowflake(execution_date=None) -> int:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    now=execution_date
    s3_path=(
        f"s3://{S3_BUCKET}/ohlcv/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    )

    hook=SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    conn=hook.get_conn()
    cursor=conn.cursos()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS
            {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            symbol VARCHAR,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            window_start TIMESTAMP_TZ,
            window_end TIMESTAMP_TZ
        );
    """)

    cursor.execute(f"""
        COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
        FROM '{s3_path}'
        FILE_FORMAT=(TYPE=PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR=CONTINUE;
    """)

    rows_loaded=cursor.fetchone()[0]
    logger.info(f"Loaded {rows_loaded} rows into snowflake from {s3_path}")
    cursor.close()
    return rows_loaded

@task
def log_load_summary(rows_loaded: int):
    if rows_loaded==0:
        logger.warning("Snowflake load completed but no rows were inserted")
    else:
        logger.info(f"Snowflake load summary: {rows_loaded} rows inserted")


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Load daily Parquet files from S3 into Snowflake"
)
def snowflake_load():
    rows=load_parquet_to_snowflake
    log_load_summary(rows)


snowflake_load()