from airflow.sdk import task, dag
from datetime import datetime, timedelta
import logging
import os

logger=logging.getLogger(__name__)

TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@timescaledb:5432/{TIMESCALE_DB}"

S3_BUCKET=os.getenv("S3_BUCKET", "finance-stock-etl-bucket")
AWS_CONN_ID="aws_s3"


@task
def extract_ohlcv_bars(execution_date=None) -> str:
    from sqlalchemy import create_engine, text
    import pandas as pd

    window_end=execution_date.replace(minute=0, second=0, microsecond=0)
    window_start=window_end-timedelta(hours=1)
    
    engine=create_engine(TIMESCALE_URL)
    with engine.connect() as conn:
        df=pd.read_sql(text("""
            SELECT *
            FROM ohlcv_bars
            WHERE window_start>=:start
                AND window_end<:end
            ORDER BY window_start ASC
        """), conn, params={
            "start": window_start.isoformat(),
            "end": window_end.isoformat()
        })
    logger.info(f"Extracted {len(df)} bars from window {window_start} -> {window_end}")
    return df.to_json(orient="records")

@task
def write_parquet_to_s3(bars_json: str, execution_date=None) -> str:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    df=pd.read_json(bars_json, orient="records")
    if df.empty:
        logger.warning("No bars to archive - skipping S3 write")
        return ""
    
    buffer=BytesIO()
    table=pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)

    now=execution_date
    key=(
        f"ohlcv/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"ohlcv_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    hook=S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_bytes(
        bytes_data=buffer.read(),
        key=key,
        bucket_name=S3_BUCKET,
        replace=True
    )

    logger.info(f"Archived {len(df)} bars to s3://{S3_BUCKET}/{key}")
    return key


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    description="Archive hourly OHLCV from TimescaleDB to S3 as Parquet"
)
def s3_parquet_archive():
    now=datetime.now()
    bars=extract_ohlcv_bars(now)
    write_parquet_to_s3(bars, now)

s3_parquet_archive()