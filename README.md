# Stock Agent

A comprehensive system for retrieving financial news, stock data, and generating stock price predictions.

## Features

- **Intent-Based Navigation**: Automatically detects user intent (retrieve news, retrieve stock data, or analyze stock)
- **News Retrieval**: Fetches and processes news articles about companies
- **Stock Data Retrieval**: Fetches and analyzes historical stock price data
- **Stock Analysis**: Combines news sentiment and technical analysis to predict stock trends
- **Multiple Execution Modes**: Intent-based, agent-based, or sequential execution
- **API and CLI Interfaces**: Access the system through a RESTful API or command-line interface

## System Architecture

The system consists of several components:

- **Intent Recognition**: Identifies the user's intent from their query
- **Company Identification**: Extracts company name and ticker symbol from the query
- **News Retrieval**: Fetches and processes news articles about the company
- **Stock Data Retrieval**: Fetches and analyzes historical stock price data
- **Stock Analysis**: Combines news and price data to generate predictions
- **Master Agent**: Coordinates the other components based on the detected intent

## Usage

### Command-Line Interface

The system provides a command-line interface for easy interaction:

```bash
# Process a natural language query using intent-based navigation
python cli.py query "What's the latest news about Microsoft?"

# Process a query with a specific intent
python cli.py intent --intent retrieve_news "Microsoft"

# Start interactive mode
python cli.py interactive
```

### API

The system also provides a RESTful API:

```bash
# Start the API server
uvicorn api.agent:app --reload

# Access the API documentation
# Open http://localhost:8000/docs in your browser
```

API endpoints:

- `POST /query`: Process a natural language query
- `POST /intent`: Process a query with a specific intent
- `GET /health`: Health check endpoint

### Execution Modes

The system supports three execution modes:

1. **Intent-Based** (default): Automatically detects the user's intent and executes the appropriate workflow
2. **Agent-Based**: Uses a ReAct agent to determine the execution flow
3. **Sequential**: Executes all steps in sequence (company identification, news retrieval, stock data retrieval, analysis)

## Examples

### Retrieve News

```
Query: "What's the latest news about Microsoft?"
```

The system will:
1. Identify the intent as "retrieve_news"
2. Extract "Microsoft" as the company name and "MSFT" as the ticker
3. Retrieve and process news articles about Microsoft

### Retrieve Stock Data

```
Query: "Show me the stock price data for AAPL"
```

The system will:
1. Identify the intent as "retrieve_stock"
2. Extract "Apple" as the company name and "AAPL" as the ticker
3. Retrieve and analyze historical stock price data for AAPL

### Analyze Stock

```
Query: "Analyze Microsoft stock and predict its trend"
```

The system will:
1. Identify the intent as "analyze_stock"
2. Extract "Microsoft" as the company name and "MSFT" as the ticker
3. Retrieve and process news articles about Microsoft
4. Retrieve and analyze historical stock price data for MSFT
5. Generate a prediction based on the news and price data

## Configuration

The system uses environment variables for configuration:

- `OPENAI_API_KEY`: OpenAI API key for LLM access
- `ELASTICSEARCH_HOST`: Elasticsearch host for news data
- `ELASTICSEARCH_PORT`: Elasticsearch port
- `ARTICLE_INDEX_NAME`: Elasticsearch index name for news articles
- `DB_HOST`: PostgreSQL host for stock data
- `DB_PORT`: PostgreSQL port
- `DB_NAME`: PostgreSQL database name
- `DB_USER`: PostgreSQL username
- `DB_PASS`: PostgreSQL password
- `PRICE_TABLE_NAME`: PostgreSQL table name for stock prices

## Development

### Project Structure

```
stock-agent/
├── api/                  # API interface
│   └── agent.py          # FastAPI application
├── core/                 # Core functionality
│   ├── __init__.py       # Environment setup
│   ├── agents.py         # Agent implementations
│   ├── executor.py       # Execution functions
│   ├── parser.py         # Output parser
│   ├── prompts.py        # Prompt templates
│   └── tools.py          # Tool implementations
├── utils/                # Utility functions
│   ├── es.py             # Elasticsearch utilities
│   └── psql.py           # PostgreSQL utilities
├── cli.py                # Command-line interface
└── README.md             # This file
```

### Adding New Intents

To add a new intent:

1. Add the intent to the `IntentRecognitionAgent` class in `core/agents.py`
2. Update the intent recognition prompt in `core/prompts.py`
3. Create a new agent class for the intent in `core/agents.py`
4. Add the intent to the `run_intent_based` method in `StockPredictionMasterAgent`
5. Update the API and CLI interfaces to support the new intent
