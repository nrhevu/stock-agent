import json
import logging
from typing import Any, Dict, List, Union

import pandas as pd
from dotenv import load_dotenv
from langchain import hub
from langchain.agents import AgentExecutor, Tool, create_react_agent
from core.prompts import (company_identifier_prompt, intent_recognition_prompt, 
                         master_agent_prompt, news_retrieval_prompt, react_prompt,
                         stock_analysis_prompt, stock_data_prompt)
from langchain.schema import StrOutputParser

from core import (llm_analysis,
                  llm_master, llm_retrieval)
from core.tools import (retrieve_news_articles,
                        retrieve_stock_prices_dataframe)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

# ===== 0. INTENT RECOGNITION AGENT =====

class IntentRecognitionAgent:
    """Agent responsible for identifying user intent from query"""
    
    # Define possible intents
    INTENT_NEWS = "retrieve_news"
    INTENT_STOCK = "retrieve_stock"
    INTENT_ANALYSIS = "analyze_stock"
    INTENT_UNKNOWN = "unknown"
    
    def __init__(self, llm=None):
        self.llm = llm or llm_retrieval
        self.chain = intent_recognition_prompt | self.llm | StrOutputParser()
    
    def run(self, query: str) -> Dict[str, Any]:
        """Identify intent from the query"""
        try:
            result = self.chain.invoke({"query": query})
            intent_data = json.loads(result)
            return {
                "intent": intent_data.get("intent", self.INTENT_UNKNOWN),
                "confidence": intent_data.get("confidence", 0.0),
                "company_focus": intent_data.get("company_focus", False)
            }
        except Exception as e:
            logger.error(f"Error in IntentRecognitionAgent: {e}", exc_info=True)
            return {
                "intent": self.INTENT_UNKNOWN,
                "confidence": 0.0,
                "company_focus": False,
                "error": str(e)
            }

# ===== 1. RETRIEVAL AGENTS =====

class CompanyIdentifierAgent:
    """Agent responsible for identifying company name and ticker symbol"""
    
    def __init__(self, llm=None):
        self.llm = llm or llm_retrieval
        self.chain = company_identifier_prompt | self.llm | StrOutputParser()
    
    def run(self, query: str) -> Dict[str, str]:
        """Identify company and ticker from the query"""
        try:
            result = self.chain.invoke({"query": query})
            company_data = json.loads(result)
            return {
                "company_name": company_data.get("company_name", "unknown"),
                "ticker": company_data.get("ticker", "unknown")
            }
        except Exception as e:
            logger.error(f"Error in CompanyIdentifierAgent: {e}", exc_info=True)
            return {"company_name": "unknown", "ticker": "unknown", "error": str(e)}


class NewsRetrievalAgent:
    """Agent responsible for retrieving and preprocessing news articles"""
    
    def __init__(self, llm=None):
        self.llm = llm or llm_retrieval
        
        # Use the prompt from prompts.py
        self.cleanup_chain = news_retrieval_prompt | self.llm | StrOutputParser()
    
    def retrieve_raw_news(self, company_name: str, days_ago: int = 30, max_results: int = 5) -> str:
        """Retrieve raw news data using the tool"""
        try:
            return retrieve_news_articles(
                company_name=company_name,
                days_ago=days_ago,
                max_results=max_results
            )
        except Exception as e:
            logger.error(f"Error retrieving news: {e}", exc_info=True)
            return f"Error retrieving news: {str(e)}"
    
    def process_news(self, company_name: str, raw_news: str) -> str:
        """Process and summarize raw news data"""
        try:
            return self.cleanup_chain.invoke({
                "company_name": company_name,
                "raw_news": raw_news
            })
        except Exception as e:
            logger.error(f"Error processing news: {e}", exc_info=True)
            return f"Error processing news: {str(e)}"
    
    def run(self, company_name: str, days_ago: int = 7, max_results: int = 5) -> Dict[str, str]:
        """Run the full news retrieval and processing pipeline"""
        if company_name.lower() == "unknown":
            return {"news_data": "No company identified to retrieve news."}
        
        raw_news = self.retrieve_raw_news(company_name, days_ago, max_results)
        processed_news = self.process_news(company_name, raw_news)
        
        return {
            "raw_news": raw_news,
            "processed_news": processed_news
        }


class StockDataRetrievalAgent:
    """Agent responsible for retrieving and preprocessing stock price data"""
    
    def __init__(self, llm=None):
        self.llm = llm or llm_retrieval
        
        # Use the prompt from prompts.py
        self.analysis_chain = stock_data_prompt | self.llm | StrOutputParser()
    
    def retrieve_price_data(self, ticker: str, days_ago: int = 90) -> Union[pd.DataFrame, str]:
        """Retrieve raw stock price data using the tool"""
        try:
            return retrieve_stock_prices_dataframe(
                ticker=ticker,
                days_ago=days_ago
            )
        except Exception as e:
            logger.error(f"Error retrieving stock data: {e}", exc_info=True)
            return f"Error retrieving stock data: {str(e)}"
    
    def summarize_price_data(self, df: pd.DataFrame) -> str:
        """Convert DataFrame to a text summary suitable for LLM analysis"""
        try:
            if isinstance(df, str):  # If an error message was returned
                return df
                
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
            daily_returns = daily_returns.astype(float)
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
    
    def analyze_price_data(self, ticker: str, price_summary: str) -> str:
        """Generate technical analysis from price summary"""
        try:
            return self.analysis_chain.invoke({
                "ticker": ticker,
                "price_summary": price_summary
            })
        except Exception as e:
            logger.error(f"Error analyzing price data: {e}", exc_info=True)
            return f"Error analyzing price data: {str(e)}"
    
    def run(self, ticker: str, days_ago: int = 90) -> Dict[str, Any]:
        """Run the full stock data retrieval and analysis pipeline"""
        if ticker.lower() == "unknown":
            return {"price_data": "No ticker symbol identified to retrieve stock data."}
        
        raw_df = self.retrieve_price_data(ticker, days_ago)
        
        if isinstance(raw_df, pd.DataFrame):
            price_summary = self.summarize_price_data(raw_df)
            technical_analysis = self.analyze_price_data(ticker, price_summary)
            
            return {
                "price_summary": price_summary,
                "technical_analysis": technical_analysis,
                "raw_df": raw_df  # Keep the DataFrame for potential further analysis
            }
        else:
            # If we got an error string instead of a DataFrame
            return {
                "price_summary": str(raw_df),
                "technical_analysis": "Unable to perform technical analysis due to data retrieval error.",
                "raw_df": None
            }


# ===== 2. ANALYSIS AGENT =====

class StockAnalysisAgent:
    """Agent responsible for analyzing news and price data to generate predictions"""
    
    def __init__(self, llm=None):
        self.llm = llm or llm_analysis
        
        # Use the prompt from prompts.py
        self.analysis_chain = stock_analysis_prompt | self.llm | StrOutputParser()
    
    def run(self, company_name: str, ticker: str, processed_news: str, technical_analysis: str) -> Dict[str, str]:
        """Generate final stock prediction based on news and technical analysis"""
        try:
            prediction = self.analysis_chain.invoke({
                "company_name": company_name,
                "ticker": ticker,
                "processed_news": processed_news,
                "technical_analysis": technical_analysis
            })
            
            return {
                "prediction": prediction
            }
        except Exception as e:
            logger.error(f"Error in stock analysis: {e}", exc_info=True)
            return {
                "prediction": f"Error generating prediction: {str(e)}"
            }


# ===== 3. MASTER AGENT =====

class StockPredictionMasterAgent:
    """Master agent that coordinates retrieval and analysis agents based on user intent"""
    
    def __init__(self, llm=None):
        self.llm = llm or llm_master
        
        # Initialize sub-agents
        self.intent_recognizer = IntentRecognitionAgent()
        self.company_identifier = CompanyIdentifierAgent()
        self.news_retriever = NewsRetrievalAgent()
        self.stock_data_retriever = StockDataRetrievalAgent()
        self.stock_analyst = StockAnalysisAgent()
        
        # Create tools for REACT agent
        self.tools = [
            Tool(
                name="identify_intent",
                func=self._identify_intent,
                description="Identifies the user's intent from the query (retrieve news, retrieve stock data, or analyze)"
            ),
            Tool(
                name="identify_company",
                func=self._identify_company,
                description="Identifies company name and ticker symbol from user query"
            ),
            Tool(
                name="retrieve_news",
                func=self._retrieve_news,
                description="Retrieves and processes news articles for a specific company"
            ),
            Tool(
                name="retrieve_stock_data",
                func=self._retrieve_stock_data,
                description="Retrieves and analyzes historical stock price data for a ticker"
            ),
            Tool(
                name="analyze_stock",
                func=self._analyze_stock,
                description="Analyzes news and price data to predict stock trend"
            )
        ]
        
        # Use the prompt from prompts.py
        self.prompt = master_agent_prompt
    
    def _identify_intent(self, query: str) -> str:
        """Tool function to identify intent from query"""
        result = self.intent_recognizer.run(query)
        return json.dumps(result)
    
    def _identify_company(self, query: str) -> str:
        """Tool function to identify company from query"""
        result = self.company_identifier.run(query)
        return json.dumps(result)
    
    def _retrieve_news(self, company_name: str, days_ago: int = 30) -> str:
        """Tool function to retrieve news for company"""
        result = self.news_retriever.run(company_name, days_ago)
        return result.get("processed_news", "Error retrieving news")
    
    def _retrieve_stock_data(self, ticker: str, days_ago: int = 90) -> str:
        """Tool function to retrieve stock data for ticker"""
        result = self.stock_data_retriever.run(ticker, days_ago)
        return result.get("technical_analysis", "Error retrieving stock data")
    
    def _analyze_stock(self, company_name: str, ticker: str, processed_news: str, technical_analysis: str) -> str:
        """Tool function to analyze all data and make prediction"""
        result = self.stock_analyst.run(company_name, ticker, processed_news, technical_analysis)
        return result.get("prediction", "Error generating prediction")
    
    def create_agent(self):
        """Create the master REACT agent"""
        # Use the prompt from prompts.py
        
        # Create the agent
        agent = create_react_agent(self.llm, self.tools, react_prompt)
        
        # Create the agent executor
        return AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=15  # Limit iterations to prevent infinite loops
        )
    
    def run_intent_based(self, query: str, news_days: int = 30, price_days: int = 90) -> Dict[str, Any]:
        """Run the pipeline based on detected intent"""
        try:
            # Step 1: Identify intent
            logger.info("Step 1: Identifying user intent")
            intent_info = self.intent_recognizer.run(query)
            intent = intent_info.get("intent")
            
            logger.info(f"Identified intent: {intent}")
            
            # Step 2: Identify company and ticker (needed for all intents)
            logger.info("Step 2: Identifying company and ticker")
            company_info = self.company_identifier.run(query)
            company_name = company_info.get("company_name")
            ticker = company_info.get("ticker")
            
            logger.info(f"Identified company: {company_name}, ticker: {ticker}")
            
            # Step 3: Execute intent-specific workflow
            if intent == IntentRecognitionAgent.INTENT_NEWS:
                # News retrieval intent
                logger.info("Executing news retrieval workflow")
                news_result = self.news_retriever.run(company_name, news_days)
                
                return {
                    "query": query,
                    "intent": intent,
                    "company_name": company_name,
                    "ticker": ticker,
                    "news_data": news_result.get("processed_news"),
                    "raw_news": news_result.get("raw_news")
                }
                
            elif intent == IntentRecognitionAgent.INTENT_STOCK:
                # Stock data retrieval intent
                logger.info("Executing stock data retrieval workflow")
                stock_result = self.stock_data_retriever.run(ticker, price_days)
                
                return {
                    "query": query,
                    "intent": intent,
                    "company_name": company_name,
                    "ticker": ticker,
                    "technical_analysis": stock_result.get("technical_analysis"),
                    "price_summary": stock_result.get("price_summary")
                }
                
            elif intent == IntentRecognitionAgent.INTENT_ANALYSIS:
                # Full analysis intent
                logger.info("Executing full analysis workflow")
                
                # Get news data
                news_result = self.news_retriever.run(company_name, news_days)
                processed_news = news_result.get("processed_news")
                
                # Get stock data
                stock_result = self.stock_data_retriever.run(ticker, price_days)
                technical_analysis = stock_result.get("technical_analysis")
                
                # Generate prediction
                prediction_result = self.stock_analyst.run(
                    company_name, 
                    ticker, 
                    processed_news, 
                    technical_analysis
                )
                
                return {
                    "query": query,
                    "intent": intent,
                    "company_name": company_name,
                    "ticker": ticker,
                    "news_data": processed_news,
                    "technical_analysis": technical_analysis,
                    "prediction": prediction_result.get("prediction"),
                    "raw_news": news_result.get("raw_news"),
                    "price_summary": stock_result.get("price_summary")
                }
                
            else:
                # Unknown intent - run full analysis as fallback
                logger.info("Unknown intent - running full analysis as fallback")
                return self.run_sequential(query, news_days, price_days)
            
        except Exception as e:
            logger.error(f"Error in intent-based execution: {e}", exc_info=True)
            return {
                "error": f"Failed to complete request: {str(e)}",
                "query": query
            }
    
    def run_sequential(self, query: str, news_days: int = 30, price_days: int = 90) -> Dict[str, Any]:
        """Run the full pipeline in sequential mode (without REACT agent)"""
        try:
            # Step 1: Identify company and ticker
            logger.info("Step 1: Identifying company and ticker")
            company_info = self.company_identifier.run(query)
            company_name = company_info.get("company_name")
            ticker = company_info.get("ticker")
            
            logger.info(f"Identified company: {company_name}, ticker: {ticker}")
            
            # Step 2: Retrieve news
            logger.info("Step 2: Retrieving news")
            news_result = self.news_retriever.run(company_name, news_days)
            processed_news = news_result.get("processed_news")
            
            # Step 3: Retrieve stock data
            logger.info("Step 3: Retrieving stock data")
            stock_result = self.stock_data_retriever.run(ticker, price_days)
            technical_analysis = stock_result.get("technical_analysis")
            
            # Step 4: Generate prediction
            logger.info("Step 4: Generating prediction")
            prediction_result = self.stock_analyst.run(
                company_name, 
                ticker, 
                processed_news, 
                technical_analysis
            )
            
            # Combine all results
            return {
                "query": query,
                "company_name": company_name,
                "ticker": ticker,
                "news_data": processed_news,
                "technical_analysis": technical_analysis,
                "prediction": prediction_result.get("prediction"),
                "raw_news": news_result.get("raw_news"),
                "price_summary": stock_result.get("price_summary")
            }
            
        except Exception as e:
            logger.error(f"Error in master agent sequential execution: {e}", exc_info=True)
            return {
                "error": f"Failed to complete prediction: {str(e)}",
                "query": query
            }
    
    def run_agent(self, query: str) -> Dict[str, Any]:
        """Run the full pipeline using the REACT agent"""
        agent = self.create_agent()
        return agent.invoke({"input": query})
    
    def run(self, query: str, news_days: int = 30, price_days: int = 90, use_agent: bool = False, use_intent: bool = True) -> Dict[str, Any]:
        """Run the pipeline using the specified method"""
        if use_agent:
            return self.run_agent(query)
        elif use_intent:
            return self.run_intent_based(query, news_days, price_days)
        else:
            return self.run_sequential(query, news_days, price_days)
