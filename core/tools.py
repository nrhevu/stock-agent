import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from dotenv import load_dotenv
from langchain import hub
from langchain.agents import AgentExecutor, create_react_agent
from langchain.pydantic_v1 import BaseModel, Field
from langchain.tools import tool
from langchain_openai import ChatOpenAI

from core import ARTICLE_INDEX_NAME, PRICE_TABLE_NAME, es_utils, pg_utils
from core.chains import analysis_chain, company_detection_chain

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Tool 1: News Retriever (Updated for news_data schema)
class NewsSearchInput(BaseModel):
    company_name: str = Field(
        description="The company name (e.g., 'google', 'microsoft') to search for news articles."
    )
    days_ago: int = Field(
        default=7, description="How many days back to search for articles."
    )
    max_results: int = Field(
        default=3, description="Maximum number of articles to retrieve for analysis."
    )


@tool("retrieve_news_articles", args_schema=NewsSearchInput)
def retrieve_news_articles(
    # str_args,
    company_name: str,
    days_ago: int = 7,
    max_results: int = 3,
) -> str:
    """Retrieves recent news articles for a specific company"""
    global es_utils
    # args = json.loads(str_args)
    # company_name: str = args.get("company_name")
    # days_ago: int = args.get("days_ago", 90)
    # max_results: int = args.get("max_results", 3)
    if not es_utils:
        return "Error: Elasticsearch utility not initialized."
    logger.info(
        f"Retrieving news for company: {company_name}, days: {days_ago}, max: {max_results}"
    )
    try:
        search_after_date = datetime.now() - timedelta(days=days_ago)
        es_query_body = {
            "query": {
                "bool": {
                    # Use 'term' or 'match' based on how 'company' is indexed (keyword vs text)
                    # 'term' is for exact keyword matches (case-sensitive unless analyzer applied)
                    # 'match' is for text search (analyzed)
                    "must": [
                        {
                            "match": {"company": company_name.lower()}
                        }  # Assuming company field is text and lowercase
                    ],
                    "filter": [
                        {
                            "range": {
                                "publish_date": {"gte": search_after_date.isoformat()}
                            }
                        }
                    ],
                }
            },
            "sort": [{"publish_date": {"order": "desc"}}],
        }
        hits = es_utils.search(
            index_name=ARTICLE_INDEX_NAME, query_body=es_query_body, size=max_results
        )
        articles_summary = []
        if not hits:
            return f"No recent news articles found for '{company_name}' in the last {days_ago} days."

        for i, hit in enumerate(hits):
            source = hit.get("_source", {})
            title = source.get(
                "title_en", source.get("title_vi", "N/A")
            )  # Prefer English title
            pub_date = source.get("publish_date", "N/A")
            # Choose content for analysis (e.g., English if available)
            content = source.get("content_en", source.get("content_vi", ""))
            snippet = (content[:400] + "...") if len(content) > 400 else content

            # IMPORTANT: Return content needed for the sentiment tool
            articles_summary.append(
                f"Article {i+1}:\n"
                f"  ID: {hit.get('_id', 'N/A')}\n"  # Include ID if needed later
                f"  Title: {title}\n"
                f"  Date: {pub_date}\n"
                f"  Content Snippet: {snippet}\n"
                f"  Full Content (for analysis): {content}\n"  # Pass full content
                f"---"
            )
        return "\n".join(articles_summary)
    except Exception as e:
        logger.error(f"Error retrieving news from Elasticsearch: {e}", exc_info=True)
        return f"Error searching Elasticsearch news: {e}"


# Tool 2: Stock Price Retriever (Returns DataFrame)
class StockPriceDFInput(BaseModel):
    ticker: str = Field(description="The stock ticker symbol (e.g., GOOGL, MSFT).")
    days_ago: int = Field(
        default=90,
        description="How many days of historical price data to retrieve for analysis (e.g., 90 for ML).",
    )


@tool("retrieve_stock_prices_dataframe", args_schema=StockPriceDFInput)
def retrieve_stock_prices_dataframe(
    # str_args,
    ticker: str,
    days_ago: int = 90,
) -> pd.DataFrame | str:
    """
    Retrieves historical stock price data for analysis.
    Returns the DataFrame object directly for use by other tools, or an error string.
    Requires columns: date, open, high, low, close, volume.
    """
    global pg_utils
    # args = json.loads(str_args)
    # ticker: str = args.get("ticker")
    # days_ago: int = args.get("days_ago", 90)
    if not pg_utils:
        return "Error: PostgreSQL utility not initialized."
    logger.info(
        f"Retrieving stock price DataFrame for ticker: {ticker}, days: {days_ago}"
    )
    try:
        start_date = datetime.now() - timedelta(days=days_ago)
        # Fetch enough data for ML preprocessing (e.g., rolling windows)
        query = f"""
        SELECT date, "open", high, low, "close", volume
        FROM {PRICE_TABLE_NAME}
        WHERE ticker = '%s' AND date >= '%s'
        ORDER BY date ASC; -- Order ASC for time series analysis
        """%(ticker.upper(), start_date.date())
        # params = (ticker.upper(), start_date.date())
        
        df = pg_utils.get_data_as_dataframe(query)

        if df.empty:
            return f"No price data found for ticker {ticker.upper()} in the last {days_ago} days to create DataFrame."

        # Basic validation (ML model might do more)
        required_cols = ["date", "open", "high", "low", "close", "volume"]
        if not all(col in df.columns for col in required_cols):
            return f"Error: Retrieved data for {ticker.upper()} is missing required columns ({required_cols})."

        logger.info(
            f"Successfully retrieved DataFrame for {ticker.upper()} with {len(df)} rows."
        )
        # *** Return the DataFrame object ***
        return df
    except Exception as e:
        logger.error(
            f"Error retrieving stock price DataFrame from Postgres: {e}", exc_info=True
        )
        return f"Error retrieving stock price DataFrame for {ticker.upper()}."

###########################################
# Create helper functions to process data #
###########################################
# These functions will be used in the agent executor to process data
def extract_company_info(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract company and ticker information from input or detection chain"""
    query = input_data.get("query", "")
    
    # If company info is already provided, use it
    if "company_name" in input_data and "ticker" in input_data:
        return input_data
    
    # Otherwise detect company info from query
    try:
        result = company_detection_chain.invoke({"input": query})
        company_data = json.loads(result)
        
        return {
            "company_name": company_data.get("company_name", "Unknown"),
            "ticker": company_data.get("ticker", "Unknown"),
            "query": query
        }
    except Exception as e:
        logger.error(f"Error detecting company info: {e}", exc_info=True)
        return {
            "company_name": "Unknown",
            "ticker": "Unknown",
            "query": query,
            "error": f"Failed to detect company info: {str(e)}"
        }

def fetch_news_data(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Retrieve news articles for the identified company"""
    company_name = input_data.get("company_name")
    days_ago = input_data.get("news_days", 7) 
    
    if company_name == "Unknown":
        return {**input_data, "news_data": "No company identified to retrieve news."}
    
    try:
        news_data = retrieve_news_articles(
            company_name=company_name,
            days_ago=days_ago,
            max_results=5
        )
        return {**input_data, "news_data": news_data}
    except Exception as e:
        logger.error(f"Error retrieving news: {e}", exc_info=True)
        return {**input_data, "news_data": f"Error retrieving news: {str(e)}"}

def fetch_stock_data(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Retrieve stock price data for the identified ticker"""
    ticker = input_data.get("ticker")
    days_ago = input_data.get("price_days", 90)
    
    if ticker == "Unknown":
        return {**input_data, "price_data": "No ticker symbol identified to retrieve stock data."}
    
    try:
        price_df = retrieve_stock_prices_dataframe(
            ticker=ticker,
            days_ago=days_ago
        )
        
        # If it's a DataFrame, convert to useful summary
        if isinstance(price_df, pd.DataFrame):
            price_data = summarize_price_data(price_df)
        else:
            price_data = str(price_df)  # Error message or other return
            
        return {**input_data, "price_data": price_data}
    except Exception as e:
        logger.error(f"Error retrieving stock data: {e}", exc_info=True)
        return {**input_data, "price_data": f"Error retrieving stock data: {str(e)}"}

def summarize_price_data(df: pd.DataFrame) -> str:
    """Convert DataFrame to a text summary suitable for LLM analysis"""
    try:
        # Basic summary stats
        summary = f"Data period: {df['date'].min()} to {df['date'].max()}\n"
        summary += f"Trading days: {len(df)}\n\n"
        
        # Calculate price changes
        start_price = df['close'].iloc[0]
        end_price = df['close'].iloc[-1]
        price_change = end_price - start_price
        percent_change = (price_change / start_price) * 100
        
        summary += f"Starting price: ${start_price:.2f}\n"
        summary += f"Latest price: ${end_price:.2f}\n"
        summary += f"Overall change: ${price_change:.2f} ({percent_change:.2f}%)\n\n"
        
        # Recent performance (last 5 days)
        if len(df) >= 5:
            recent = df.tail(5)
            recent_change = recent['close'].iloc[-1] - recent['close'].iloc[0]
            recent_pct = (recent_change / recent['close'].iloc[0]) * 100
            summary += f"Last 5 days change: ${recent_change:.2f} ({recent_pct:.2f}%)\n\n"
        
        # Volatility measure
        daily_returns = df['close'].pct_change().dropna()
        volatility = daily_returns.std() * 100
        summary += f"Daily volatility: {volatility:.2f}%\n\n"
        
        # Volume analysis
        avg_volume = df['volume'].mean()
        recent_vol = df['volume'].tail(5).mean()
        vol_change_pct = ((recent_vol - avg_volume) / avg_volume) * 100
        summary += f"Average daily volume: {avg_volume:.0f}\n"
        summary += f"Recent volume: {recent_vol:.0f} ({vol_change_pct:.2f}% vs avg)\n\n"
        
        # Key price levels
        summary += f"52-day high: ${df['high'].max():.2f}\n"
        summary += f"52-day low: ${df['low'].min():.2f}\n\n"
        
        # Recent prices table
        summary += "Recent price history (last 10 days):\n"
        for _, row in df.tail(10).iterrows():
            summary += f"{row['date'].strftime('%Y-%m-%d')}: Open ${row['open']:.2f}, High ${row['high']:.2f}, Low ${row['low']:.2f}, Close ${row['close']:.2f}, Volume {row['volume']:.0f}\n"
        
        return summary
    except Exception as e:
        logger.error(f"Error summarizing price data: {e}", exc_info=True)
        return f"Error processing price data: {str(e)}"