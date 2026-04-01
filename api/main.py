import os
from dotenv import load_dotenv
from fastapi import FastAPI
from prometheus_client import make_asgi_app
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from api.routes import price, ohlcv, sentiment

load_dotenv()

provider=TracerProvider()
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)

app=FastAPI(
    title="Finance ETL API",
    description="Read layer for the real-time finance pipeline",
    version="1.0.0"
)

metrics_app=make_asgi_app()
app.mount("/metrics", metrics_app)

app.include_router(price.router, prefix="/price", tags=["price"])
app.include_router(ohlcv.router, prefix="/ohlcv", tags=["ohlcv"])
app.include_router(sentiment.router, prefix="/sentiment", tags=["sentiment"])


@app.get("/health")
def health():
    return {"status": "ok"}
