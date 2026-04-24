from airflow.sdk import task, dag
from datetime import datetime
from dotenv import load_dotenv
import logging
import os

load_dotenv()
logger=logging.getLogger(__name__)

S3_BUCKET=os.getenv("S3_BUCKET", "finance-stock-etl-bucket")
SNOWFLAKE_CONN="snowflake_default"
SNOWFLAKE_TABLE="fact_ohlcv"
SNOWFLAKE_SCHEMA="FINANCE"
SNOWFLAKE_DB="FINANCE_DB"

@task
def load_parquet_to_snowflake(execution_date=None) -> int:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    now=execution_date
    s3_path=(
        f"@FINANCE_STOCK_STAGE/ohlcv/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    )

    hook=SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
    conn=hook.get_conn()
    cursor=conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS
            {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            symbol VARCHAR,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            window_start TIMESTAMPTZ(0),
            window_end TIMESTAMPTZ(0)
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
    now=datetime.now()
    rows=load_parquet_to_snowflake(now)
    log_load_summary(rows)


snowflake_load()