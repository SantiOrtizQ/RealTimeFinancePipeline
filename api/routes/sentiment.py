import os
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

router=APIRouter()

TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@localhost:5432/{TIMESCALE_DB}"

engine=create_engine(TIMESCALE_URL)


@router.get("/{symbol}")
def get_sentiment(
    symbol: str,
    date: str=Query(..., description="Date in YYYY-MM-DD format"),
    limit: int=Query(20, le=100)
):
    symbol=symbol.upper()
    try:
        with engine.connect() as conn:
            rows=conn.execute(text("""
                SELECT
                    article_id,
                    title,
                    source,
                    url,
                    compound,
                    sentiment,
                    published_at
                FROM news_sentiment
                WHERE :symbol=ANY(symbols)
                    AND published_at::date=:date::DATE
                ORDER BY published_at DESC
                LIMIT :limit
            """), {
                "symbol": symbol,
                "date": date,
                "limit": limit
            }).fetchall()
        
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No sentiment data found for {symbol} on {date}"
            )
        
        avg_compound=sum(r.compound for r in rows)/len(rows)

        return {
            "symbol": symbol,
            "date": date,
            "avg_compound": round(avg_compound, 4),
            "articles": [
                {
                    "article_id": row.article_id,
                    "title": row.title,
                    "source": row.source,
                    "url": row.url,
                    "compound": row.compound,
                    "sentiment": row.sentiment,
                    "published_at": row.published_at.isoformat()
                }
                for row in rows
            ],
            "count": len(rows)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))