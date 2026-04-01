import os
import redis as redis_client
from fastapi import APIRouter, HTTPException
from dotenv import load_dotenv

load_dotenv()

router=APIRouter()
redis=redis_client.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

@router.get("/{symbol}")
def get_latest_price(symbol: str):
    symbol=symbol.upper()
    price=redis.get(f"ticker:{symbol}")
    if price is None:
        raise HTTPException(
            status_code=404,
            detail=f"No cached price found for {symbol}. "
                f"Is the producer running?"
        )
    return {
        "symbol": symbol,
        "price": float(price),
        "source": "redis_cache"
    }