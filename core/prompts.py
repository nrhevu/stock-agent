import logging

from langchain import hub
from langchain.prompts import ChatPromptTemplate

from core.tools import retrieve_news_articles, retrieve_stock_prices_dataframe

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# List the retrieval tools
tools = [
    retrieve_news_articles,
    retrieve_stock_prices_dataframe
]

# Get the ReAct prompt template for the agent
react_prompt = hub.pull("hwchase17/react")

# Intent-specific prompts
company_identifier_prompt = ChatPromptTemplate.from_template("""
You are a financial entity recognition specialist. Extract the company name and stock ticker from the query.

USER QUERY: {query}

Return ONLY a JSON object with these fields:
- company_name: The full name of the company
- ticker: The stock ticker symbol

If uncertain about any field, use "unknown" as the value.
""")

intent_recognition_prompt = ChatPromptTemplate.from_template("""
You are an intent recognition specialist for a financial assistant. Determine the primary intent of the user's query.

USER QUERY: {query}

Possible intents:
1. retrieve_news - User wants news articles about a company
2. retrieve_stock - User wants stock price data for a company
3. analyze_stock - User wants analysis combining news and stock data

Return ONLY a JSON object with these fields:
- intent: The primary intent (one of: retrieve_news, retrieve_stock, analyze_stock)
- confidence: A number between 0 and 1 indicating your confidence
- company_focus: true/false - whether the query focuses on a specific company

If the intent is unclear, use "unknown" as the intent value.
""")

news_retrieval_prompt = ChatPromptTemplate.from_template("""
You are a financial news curator. Review these news articles about {company_name} and extract the most relevant 
information for stock price prediction.

ORIGINAL NEWS DATA:
{raw_news}

Create a concise summary of the key points, focusing on:
1. Market sentiment (positive/negative/neutral)
2. Major news events or announcements
3. Financial performance indicators
4. Industry trends affecting the company

FORMAT YOUR RESPONSE AS:
SUMMARY:
[Your concise summary here]

KEY DEVELOPMENTS:
- [Key point 1]
- [Key point 2]
- [Key point 3]

SENTIMENT: [Overall sentiment: POSITIVE/NEGATIVE/NEUTRAL/MIXED]
""")

stock_data_prompt = ChatPromptTemplate.from_template("""
You are a technical stock analyst. Review this price data for {ticker} and extract key technical indicators and patterns.

RAW PRICE DATA SUMMARY:
{price_summary}

Create a concise technical analysis, focusing on:
1. Overall trend (bullish/bearish/neutral)
2. Key support and resistance levels
3. Volume patterns and anomalies
4. Recent momentum indicators
5. Volatility analysis

FORMAT YOUR RESPONSE AS:
TECHNICAL INDICATORS:
[Your concise technical analysis here]

KEY LEVELS:
- Support: [list key support levels]
- Resistance: [list key resistance levels]

VOLUME ANALYSIS:
[Brief volume analysis]

MOMENTUM: [INCREASING/DECREASING/STABLE]
""")

stock_analysis_prompt = ChatPromptTemplate.from_template("""
# Stock Trend Analysis and Prediction

## Company Information
- Company Name: {company_name}
- Ticker Symbol: {ticker}

## News Analysis
{processed_news}

## Technical Analysis
{technical_analysis}

## Your Task
As a financial analyst, synthesize the news sentiment and technical analysis to predict 
the stock price direction for {ticker} in the coming days.

Follow these steps:
1. Compare the news sentiment with technical indicators
2. Identify confirmation or contradiction between news and price data
3. Determine the most likely price direction based on all available information
4. Assign a confidence level to your prediction
5. Explain your reasoning in clear, concise terms

FORMAT YOUR RESPONSE AS:

# PREDICTION FOR {ticker}

## DIRECTION: [UP/DOWN/NEUTRAL]

## CONFIDENCE: [HIGH/MEDIUM/LOW]

## RATIONALE:
[Your concise explanation of 2-3 paragraphs]

## KEY FACTORS:
1. [Factor 1]
2. [Factor 2]
3. [Factor 3]
4. [Factor 4]

## POTENTIAL RISKS:
- [Risk 1]
- [Risk 2]
""")

# Master agent prompt with intent recognition
master_agent_prompt = ChatPromptTemplate.from_template("""
You are MasterStock, an expert financial system coordinator. Your role is to handle user requests about stocks and financial data.

USER QUERY: {query}

To handle this request effectively:

1. First, identify the user's intent (retrieve news, retrieve stock data, or analyze)
2. Then identify the company and ticker symbol
3. Based on the intent, execute the appropriate workflow:
   - For news retrieval: gather and process news data
   - For stock data retrieval: gather and analyze stock price history
   - For analysis: gather both news and stock data, then generate a prediction

As you progress through each step, maintain context by referencing the results from previous steps.
Be thorough but avoid unnecessary repetition.

NOTE: You must execute tasks in the proper sequence - don't try to analyze before you have the necessary data.

Begin by identifying the intent, then proceed step by step.
""")

# Customize the ReAct prompt template for the agent
react_prompt.template = """
You are an AI assistant designed for financial data retrieval and analysis. Your goal is to help users get information about stocks and companies.
You MUST use the provided tools following the exact Thought -> Action -> Action Input -> Observation cycle.

Available Tools:
{tools}

Tool Usage Flow:
1. First, identify the user's intent using the `identify_intent` tool.
2. Then identify the company and ticker symbol using the `identify_company` tool.
3. Based on the intent:
   - For news retrieval: use `retrieve_news` to get news articles
   - For stock data retrieval: use `retrieve_stock_data` to get price data
   - For analysis: use both retrieval tools, then use `analyze_stock` to generate a prediction

4. Follow the ReAct cycle strictly for each step needed to gather information.

**IMPORTANT RULES FOR OUTPUT FORMAT:**
- Every 'Thought:' MUST be followed immediately by an 'Action:' block.
- The final response after gathering all data MUST start with 'Thought:', followed by 'Final Answer:'.
- Be thorough but avoid unnecessary repetition.

Begin!

Question: {input}
Thought: {agent_scratchpad}
"""
