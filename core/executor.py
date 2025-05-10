import logging

from langchain import hub
from langchain.agents import AgentExecutor, create_react_agent

from core.parser import CustomOutputParser
from core.agents import StockPredictionMasterAgent

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Create Agent and Executor ---
stock_agent = StockPredictionMasterAgent()

# Create the agent executor for ReAct agent-based execution
agent_executor = stock_agent.create_agent()

# Define execution functions for different modes
def execute_query(query, mode="intent", news_days=90, price_days=90):
    """
    Execute a user query using the specified mode.
    
    Args:
        query (str): The user's query
        mode (str): Execution mode - "intent" (default), "agent", or "sequential"
        news_days (int): Number of days to look back for news
        price_days (int): Number of days to look back for stock prices
        
    Returns:
        dict: Results of the query execution
    """
    logger.info(f"Executing query in {mode} mode: {query}")
    
    try:
        if mode == "agent":
            # Use the ReAct agent
            return stock_agent.run_agent(query)
        elif mode == "sequential":
            # Use the sequential execution path
            return stock_agent.run_sequential(query, news_days, price_days)
        else:
            # Default to intent-based execution
            return stock_agent.run_intent_based(query, news_days, price_days)
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
        return {
            "error": f"Failed to process query: {str(e)}",
            "query": query
        }

def execute_intent(query, intent, news_days=90, price_days=90):
    """
    Execute a specific intent directly, bypassing intent recognition.
    
    Args:
        query (str): The user's query
        intent (str): The specific intent to execute
        news_days (int): Number of days to look back for news
        price_days (int): Number of days to look back for stock prices
        
    Returns:
        dict: Results of the intent execution
    """
    logger.info(f"Executing specific intent '{intent}' for query: {query}")
    
    try:
        # Identify company and ticker
        company_info = stock_agent.company_identifier.run(query)
        company_name = company_info.get("company_name")
        ticker = company_info.get("ticker")
        
        if intent == "retrieve_news":
            print(f"Intent: {intent}, Company: {company_name}, Ticker: {ticker}")
            # Execute news retrieval
            news_result = stock_agent.news_retriever.run(company_name, news_days)
            return {
                "intent": intent,
                "company_name": company_name,
                "ticker": ticker,
                "news_data": news_result.get("processed_news"),
                "raw_news": news_result.get("raw_news"),
                "result": news_result.get("processed_news") + "\n" + news_result.get("raw_news")
            }
            
        elif intent == "retrieve_stock":
            print(f"Intent: {intent}, Company: {company_name}, Ticker: {ticker}")

            # Execute stock data retrieval
            stock_result = stock_agent.stock_data_retriever.run(ticker, price_days)
            return {
                "intent": intent,
                "company_name": company_name,
                "ticker": ticker,
                "technical_analysis": stock_result.get("technical_analysis"),
                "price_summary": stock_result.get("price_summary"),
                "result": stock_result.get("technical_analysis") + "\n" + stock_result.get("price_summary")
            }
            
        elif intent == "analyze_stock":
            print(f"Intent: {intent}, Company: {company_name}, Ticker: {ticker}")

            # Execute full analysis
            news_result = stock_agent.news_retriever.run(company_name, news_days)
            processed_news = news_result.get("processed_news")
            
            stock_result = stock_agent.stock_data_retriever.run(ticker, price_days)
            technical_analysis = stock_result.get("technical_analysis")
            
            prediction_result = stock_agent.stock_analyst.run(
                company_name, 
                ticker, 
                processed_news, 
                technical_analysis
            )
            
            return {
                "intent": intent,
                "company_name": company_name,
                "ticker": ticker,
                "news_data": processed_news,
                "technical_analysis": technical_analysis,
                "prediction": prediction_result.get("prediction"),
                "result": technical_analysis + "\n" + prediction_result.get("prediction")
            }
        else:
            return {
                "error": f"Unknown intent: {intent}",
                "query": query
            }
    except Exception as e:
        logger.error(f"Error executing intent: {e}", exc_info=True)
        return {
            "error": f"Failed to execute intent: {str(e)}",
            "query": query,
            "intent": intent
        }
