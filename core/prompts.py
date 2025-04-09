import logging

from langchain import hub
from langchain.prompts import ChatPromptTemplate

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

company_detection_prompt = ChatPromptTemplate.from_template(
    """
    You are a financial entity recognition expert. Your task is to identify the company name and its stock ticker symbol from the user query.

    USER QUERY: {query}

    Instructions:
    1. Identify the company being discussed in the query
    2. Determine the correct stock ticker symbol for this company
    3. If multiple companies are mentioned, focus on the main company of interest
    4. If no specific company is mentioned, respond with "Unknown" for both fields

    Return your response in the following JSON format:
    ```json
    {
        "company_name": "Full Company Name",
        "ticker": "TICKER"
    }
    ```
    """
)

analysis_prompt = ChatPromptTemplate.from_template(
    """
    # Stock Analysis and Prediction Task

    You are a financial analyst specializing in stock market predictions based on news sentiment and technical analysis.

    ## Company Information
    - Company: {company_name}
    - Ticker: {ticker}

    ## News Data
    {news_data}

    ## Stock Price Data
    {price_data}

    ## Your Task
    Analyze the provided news and stock price data to predict whether the stock price will trend UP, DOWN, or STAY NEUTRAL in the coming days.

    Follow these steps in your analysis:

    1. **News Analysis**:
    - Determine the overall sentiment of recent news (positive, negative, or neutral)
    - Identify key developments that could impact stock price
    - Note any market reactions mentioned in the articles

    2. **Technical Analysis**:
    - Examine recent price trends and momentum
    - Analyze volume patterns
    - Consider volatility and price movements
    - Identify potential support/resistance levels

    3. **Integrated Prediction**:
    - Synthesize news sentiment with technical indicators
    - Consider which factors might have stronger influence
    - Make a final prediction with supporting rationale

    ## Required Output Format
    Provide your analysis and prediction in this structured format:

    NEWS ANALYSIS:
    [Provide a concise summary of news sentiment and key points]

    TECHNICAL ANALYSIS:
    [Provide a concise summary of key technical indicators and patterns]

    PREDICTION: [UP/DOWN/STAY]
    CONFIDENCE: [HIGH/MEDIUM/LOW]
    RATIONALE:
    [Provide a clear explanation of your reasoning]

    KEY FACTORS:
    - [List 3-5 key factors that influenced your prediction]
    """
)

react_prompt = hub.pull("hwchase17/react")