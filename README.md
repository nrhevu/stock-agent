# Stock Agent

A comprehensive system for retrieving financial news, stock data, and generating stock price predictions.

## Installation

1. Start Docker (use the appropriate file for your system architecture):
   ```bash
   # For AMD64 architecture
   docker compose -f docker/docker-compose-amd64.yml up -d
   
   # For ARM64 architecture
   docker compose -f docker/docker-compose-arm64.yml up -d
   ```

2. Build Docker image:
   ```bash
   docker build -t nrhevu/stock-runner:v1.0 .
   ```

3. Install dependencies:
   ```bash
   conda create -n stockagent python=3.11
   pip install -r requirements.txt
   ```

4. Export OpenAI API key:
   ```bash
   export OPENAI_API_KEY=your_api_key
   ```

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

# Usage

## Running the Application

### Running UI 
1. Set the OpenAI API key:
   ```bash
   export OPENAI_API_KEY=your_OPENAI_API_KEY
   ```

2. Run the Streamlit app:
   ```bash
   streamlit run ui/app.py
   ```

Application Ports

- **Streamlit UI**: http://localhost:8501

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


### Execution Modes

The system supports three execution modes:

1. **Intent-Based** (default): Automatically detects the user's intent and executes the appropriate workflow
2. **Agent-Based**: Uses a ReAct agent to determine the execution flow
3. **Sequential**: Executes all steps in sequence (company identification, news retrieval, stock data retrieval, analysis)

## Data Crawling

### Crawl News Data
```bash
scrapy data_ingestion/new_crawlers/crawl crawler -a keyword={{company_id}} -o path/to/news_data/{{company_id}}.json
```

### Crawl Stock Data
```bash
python data_ingestion/stock_api.py --save-dir path/to/stock_data --tickers {{TICKER_ID}}
```

### Airflow Orchestration

The system uses Apache Airflow for orchestrating data collection and processing tasks. Two main DAGs are available:

### 1. News Data Crawling (crawl_news_data)
This DAG runs daily and performs the following tasks:
- Creates a date-based folder structure for storing news data
- Crawls news data for specific companies (NVIDIA, Google, Microsoft)
- Processes the collected news data and loads it into Elasticsearch

### 2. Stock Data Crawling (crawl_stock_data)
This DAG runs daily and performs the following tasks:
- Creates a date-based folder structure for storing stock data
- Crawls stock price data for configured tickers
- Processes the collected stock data and loads it into PostgreSQL

You can access the Airflow UI at http://localhost:8080 to monitor and manage these workflows.

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

# Development

## Project Structure

```
stock-agent/
├── .env                  # Environment variables
├── .gitignore            # Git ignore file
├── cli.py                # Command-line interface
├── crawl_daily_data.py   # Script for daily data crawling
├── docker-compose.yml    # Docker Compose configuration
├── Dockerfile            # Docker image definition
├── process_news_data.py  # News data processing script
├── process_stock_data.py # Stock data processing script
├── README.md             # Project documentation
├── requirements.txt      # Python dependencies
├── api/                  # API interface
│   └── agent.py          # FastAPI application
├── config/               # Configuration files
│   └── airflow.cfg       # Airflow configuration
├── core/                 # Core functionality
│   ├── __init__.py       # Environment setup
│   ├── agents.py         # Agent implementations
│   ├── executor.py       # Execution functions
│   ├── parser.py         # Output parser
│   ├── prompts.py        # Prompt templates
│   └── tools.py          # Tool implementations
├── dags/                 # Airflow DAGs directory
├── data/                 # Data storage directory
├── data_ingestion/       # Data ingestion components
│   ├── stock_api.py      # Stock data API client
│   └── new_crawlers/     # News crawlers
│       ├── README.MD     # Crawler documentation
│       ├── scrapy.cfg    # Scrapy configuration
│       └── crawler/      # Crawler implementation
│           ├── __init__.py
│           ├── items.py  # Scrapy items
│           ├── middlewares.py # Scrapy middlewares
│           ├── pipelines.py   # Scrapy pipelines
│           ├── settings.py    # Scrapy settings
│           └── spiders/       # Scrapy spiders
│               ├── __init__.py
│               └── crawler_spider.py # Main crawler spider
├── data_processing/      # Data processing components
│   └── feature_extraction.py # Feature extraction utilities
├── docker/               # Docker configurations
│   ├── docker-compose-amd64.yml # AMD64 Docker Compose
│   └── docker-compose-arm64.yml # ARM64 Docker Compose
├── feast/                # Feast feature store
│   └── stock/            # Stock feature repository
│       ├── __init__.py
│       ├── .gitignore
│       ├── README.md
│       └── feature_repo/ # Feature definitions
│           ├── __init__.py
│           ├── example_repo.py
│           ├── feature_store.yaml
│           ├── test_workflow.py
│           └── data/     # Feature data
├── logs/                 # Log files directory
├── ml/                   # Machine learning components
│   ├── infer.py          # Inference script
│   ├── model.py          # Model definitions
│   └── train.py          # Training script
├── nlp/                  # Natural language processing
│   ├── analyzer.py       # Text analysis utilities
│   └── translate.py      # Translation utilities
├── orchestration/        # Workflow orchestration
│   ├── dags/             # Airflow DAGs
│   │   ├── crawl_news_data.py  # News crawling DAG
│   │   ├── crawl_stock_data.py # Stock data crawling DAG
│   │   
│   └── plugins/          # Airflow plugins
├── plugins/              # System plugins
├── scripts/              # Utility scripts
├── tests/                # Test suite
│   ├── es_test.py        # Elasticsearch tests
│   ├── nlp_test.py       # NLP tests
│   └── psql_test.py      # PostgreSQL tests
├── ui/                   # User interface
│   └── app.py            # Streamlit application
└── utils/                # Utility functions
    ├── __init__.py
    ├── es.py             # Elasticsearch utilities
    └── psql.py           # PostgreSQL utilities
```

### Adding New Intents

To add a new intent:

1. Add the intent to the `IntentRecognitionAgent` class in `core/agents.py`
2. Update the intent recognition prompt in `core/prompts.py`
3. Create a new agent class for the intent in `core/agents.py`
4. Add the intent to the `run_intent_based` method in `StockPredictionMasterAgent`
5. Update the API and CLI interfaces to support the new intent
