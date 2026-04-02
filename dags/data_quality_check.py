from airflow.sdk import task, dag
from datetime import datetime
import logging
import os


logger=logging.getLogger(__name__)

TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@timescale:5432/{TIMESCALE_DB}"


@task
def run_ohlcv_quality_checks() -> dict:
    from sqlalchemy import create_engine, text

    engine=create_engine(TIMESCALE_URL)
    results={}

    with engine.connect() as conn:
        row=conn.execute(text("""
            SELECT COUNT(*) FROM ohlcv_bars
            WHERE close<open*0.5 OR close>open*2.0
        """)).scalar()
        results["extreme_price_moves"]=int(row)

        row=conn.execute(text("""
            SELECT COUNT(*) FROM ohlcv_bars
            WHERE volume<=0
        """)).scalar()
        results["zero_or_negative_volume"]=int(row)

        row=conn.execute(text("""
            SELECT COUNT(*) FROM ohlcv_bars
            WHERE window_start>NOW()
        """)).scalar()
        results["future_timestamps"]=int(row)

        row=conn.execute(text("""
            SELECT COUNT(*) FROM ohlcv_bars
            WHERE window_start<NOW()-INTERVAL '10 minutes'
                AND window_start>NOW()-INTERVAL '70 minutes'
        """)).scalar()
        results["recent_bars_count"]=int(row)
    
    logger.info(f"OHLCV quality results: {results}")
    return results

@task
def run_sentiment_quality_checks() -> dict:
    from sqlalchemy import create_engine, text

    engine=create_engine()
    results={}

    with engine.connect() as conn:
        row=conn.execute(text("""
            SELECT COUNT(*) FROM news_sentiment
            WHERE compound<-1.0 OR compound>1.0
        """)).scalar()
        results["compound_out_of_range"]=int(row)

        row=conn.execute(text("""
            SELECT COUNT(*) FROM news_sentiment
            WHERE title='' OR title IS NULL
        """)).scalar()
        results["empty_titles"]=int(row)

        row=conn.execute(text("""
            SELECT COUNT(*) FROM news_sentiment
            WHERE published_at>NOW()
        """)).scalar()
        results["future_articles"]=int(row)

    logger.info(f"Sentiment quality results: {results}")
    return results



@task
def evaluate_results(ohlcv_results: dict, sentiment_results: dict):
    failures=[]

    if ohlcv_results["high_below_low"]>0:
        failures.appned(
            f"high_below_low: {ohlcv_results['high_below_low']} rows"
        )
    if ohlcv_results["zero_or_negative_volume"]>0:
        failures.append(
            f"zero_or_negative_volume: {ohlcv_results['zero_or_negative_volume']} rows"
        )
    if ohlcv_results["future_timestamps"]>0:
        failures.append(
            f"future_timestamps: {ohlcv_results['future_timestamps']} rows"
        )
    if sentiment_results["compound_out_of_range"]>0:
        failures.append(
            f"compound_out_of_range: {sentiment_results['compound_out_of_range']} rows"
        )
    if sentiment_results["empty_titles"]>0:
        failures.append(
            f"empty_titles: {sentiment_results['empty_titles']} rows"
        )
    
    if failures:
        failure_str=" | ".join(failures)
        logger.error(f"DATA QUALITY FAILURES: {failure_str}")
        raise ValueError(f"Data quality checks failed: {failure_str}")
    else:
        logger.info("All data quality checks passed")


@dag(
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    description="Daily data quality checks on OHLCV and sentiment tables"
)
def data_quality_check():
    ohlcv_results=run_ohlcv_quality_checks()
    sentiment_results=run_sentiment_quality_checks()
    evaluate_results(ohlcv_results, sentiment_results)

data_quality_check()