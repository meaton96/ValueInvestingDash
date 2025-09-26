from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException


app = FastAPI(
    title="Value Investing Data API",
    description="Endpoints for fetching market data leveraged by the ValueInvestingDash app.",
    version="0.1.0",
    
)


@app.get("/health", summary="Simple service health check", tags=["Status"])
async def health_check() -> Dict[str, str]:
    """Return a simple heartbeat payload for monitoring purposes."""

    return {"status": "ok"}





@app.get(
    "/securities",
    summary="Fetch security metadata",
    tags=["Securities"],
)
async def read_securities(limit: int = 10) -> Dict[str, Any]:
    """Fetch a limited list of securities.

    This is a stub implementation that returns mock data and documents the shape of the
    response the real implementation should adhere to.
    """

    if limit <= 0:
        raise HTTPException(status_code=400, detail="Limit must be positive")

    sample_data: List[Dict[str, Any]] = [
        {
            "ticker": "BRK.A",
            "name": "Berkshire Hathaway Inc.",
            "sector": "Financial Services",
        },
        {
            "ticker": "MSFT",
            "name": "Microsoft Corporation",
            "sector": "Technology",
        },
    ]

    return {
        "data": sample_data[:limit],
        "count": min(limit, len(sample_data)),
    }
