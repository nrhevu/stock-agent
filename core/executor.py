import json
import logging

import pandas as pd
from langchain import hub
from langchain.agents import AgentExecutor, create_react_agent
from langchain.tools import Tool

from core import llm
from core.chains import analysis_chain
from core.parser import CustomOutputParser
from core.prompts import react_prompt
from core.tools import (extract_company_info, retrieve_news_articles,
                        retrieve_stock_prices_dataframe, summarize_price_data)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

tools = [
        # Tool to identify company/ticker
        Tool(**{
            "name": "identify_company",
            "description": "Identifies company name and ticker symbol from text",
            "args": {"query": "str"},
            "func": lambda query: json.dumps(extract_company_info({"query": query}))
        }),
        # Tool to retrieve news
        Tool(**{
            "name": "get_company_news",
            "description": "Retrieves recent news articles for a specific company",
            "args": {"company_name": "str", "days_ago": "int"},
            "func": lambda company_name, days_ago=7: retrieve_news_articles(company_name, days_ago, 5)
        }),
        # Tool to retrieve stock prices
        Tool(**{
            "name": "get_stock_prices",
            "description": "Retrieves historical stock price data for analysis",
            "args": {"ticker": "str", "days_ago": "int"},
            "func": lambda ticker, days_ago=90: summarize_price_data(
                retrieve_stock_prices_dataframe(ticker, days_ago)
                if not isinstance(retrieve_stock_prices_dataframe(ticker, days_ago), str)
                else pd.DataFrame()
            )
        }),
        # Tool to analyze and predict
        Tool(**{
            "name": "predict_stock_trend",
            "description": "Analyzes news and price data to predict stock trend",
            "args": {"company_name": "str", "ticker": "str", "news_data": "str", "price_data": "str"},
            "func": lambda company_name, ticker, news_data, price_data: analysis_chain.invoke({
                "company_name": company_name,
                "ticker": ticker,
                "news_data": news_data,
                "price_data": price_data
            })
        })
    ]
# --- Create Agent and Executor ---
agent_executor: AgentExecutor = None
try:
    agent = create_react_agent(llm=llm, tools=tools, prompt=react_prompt, output_parser=CustomOutputParser())
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        handle_parsing_errors=True, # Keep this for robustness
        max_iterations=10,
        early_stopping_method="generate",
    )
    logger.info("Langchain retrieval agent created successfully.")
except Exception as e:
    logger.critical(f"Failed to create Langchain agent: {e}", exc_info=True)
    agent_executor = None