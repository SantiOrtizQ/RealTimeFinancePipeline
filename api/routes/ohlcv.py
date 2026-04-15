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
def get_ohlcv(
    symbol: str,
    start: str=Query(..., description="ISO datetime e.g. 2024-01-01T09:30:00"),
    end: str=Query(..., description="ISO datetime e.g. 2024-01-01T16:00:00"),
    limit: int=Query(100, le=1000)
):
    symbol=symbol.upper()
    try:
        with engine.connect() as conn:
            rows=conn.execute(text("""
                SELECT
                    symbol,
                    open, high, low, close, volume,
                    window_start, window_end
                FROM ohlcv_bars
                WHERE symbol = :symbol
                  AND window_start >= :start::timestamptz
                  AND window_end   <= :end::timestamptz
                ORDER BY window_start ASC
                LIMIT :limit
            """), {
                "symbol": symbol,
                "start":  start,
                "end":    end,
                "limit":  limit,
            }).fetchall()
        
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No OHLCV data found for {symbol} in the given range"
            )
        
        return {
            "symbol": symbol,
            "bars": [
                {
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "window_start": row.window_start.isoformat(),
                    "window_end": row.window_end.isoformat()
                }
                for row in rows
            ],
            "count": len(rows)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))