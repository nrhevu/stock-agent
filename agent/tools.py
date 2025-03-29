import json
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from langchain import hub
from langchain.agents import AgentExecutor, create_react_agent
from langchain.pydantic_v1 import BaseModel, Field
from langchain.tools import tool
from langchain_openai import ChatOpenAI

from agent import ARTICLE_INDEX_NAME, PRICE_TABLE_NAME, es_utils, pg_utils

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
    str_args
) -> str:
    """Searches Elasticsearch index 'news_data' for articles about a specific company."""
    global es_utils
    args = json.loads(str_args)
    company_name: str = args.get("company_name")
    days_ago: int = args.get("days_ago", 90)
    max_results: int = args.get("max_results", 3)
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
    str_args
) -> pd.DataFrame | str:
    """
    Retrieves historical stock price data from Postgres DB as a Pandas DataFrame.
    Returns the DataFrame object directly for use by other tools, or an error string.
    Requires columns: date, open, high, low, close, volume.
    """
    global pg_utils
    args = json.loads(str_args)
    ticker: str = args.get("ticker")
    days_ago: int = args.get("days_ago", 90)
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
