import json
import logging
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from core.executor import execute_query, execute_intent

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Stock Agent API",
    description="API for retrieving stock data, news, and analysis using intent-based navigation",
    version="1.0.0"
)

# Define request and response models
class QueryRequest(BaseModel):
    query: str
    mode: Optional[str] = "intent"  # "intent", "agent", or "sequential"
    news_days: Optional[int] = 30
    price_days: Optional[int] = 90

class IntentRequest(BaseModel):
    query: str
    intent: str  # "retrieve_news", "retrieve_stock", or "analyze_stock"
    news_days: Optional[int] = 30
    price_days: Optional[int] = 90

class QueryResponse(BaseModel):
    result: Dict[str, Any]

# Define API endpoints
@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """
    Process a natural language query using the specified mode.
    
    - **query**: The user's natural language query
    - **mode**: Execution mode - "intent" (default), "agent", or "sequential"
    - **news_days**: Number of days to look back for news
    - **price_days**: Number of days to look back for stock prices
    """
    try:
        logger.info(f"Processing query: {request.query}")
        result = execute_query(
            query=request.query,
            mode=request.mode,
            news_days=request.news_days,
            price_days=request.price_days
        )
        return {"result": result}
    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")

@app.post("/intent", response_model=QueryResponse)
async def process_intent(request: IntentRequest):
    """
    Process a query with a specific intent, bypassing intent recognition.
    
    - **query**: The user's query
    - **intent**: The specific intent to execute ("retrieve_news", "retrieve_stock", or "analyze_stock")
    - **news_days**: Number of days to look back for news
    - **price_days**: Number of days to look back for stock prices
    """
    try:
        logger.info(f"Processing intent '{request.intent}' for query: {request.query}")
        result = execute_intent(
            query=request.query,
            intent=request.intent,
            news_days=request.news_days,
            price_days=request.price_days
        )
        return {"result": result}
    except Exception as e:
        logger.error(f"Error processing intent: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing intent: {str(e)}")

@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return {"status": "healthy", "message": "Stock Agent API is operational"}

# Run the app with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
