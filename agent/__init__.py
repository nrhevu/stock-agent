import logging
import os

from dotenv import load_dotenv  # Good practice to load here if utils use it
from langchain_openai import ChatOpenAI

from utils.es import ElasticsearchUtils
from utils.psql import PostgresUtils

# --- Environment Setup & Configuration ---
load_dotenv()  # Load .env file variables for use here and potentially by utils
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- !! IMPORTANT !! ---
# Load credentials securely (Environment Variables Recommended)
# OpenAI
# os.environ["OPENAI_API_KEY"] = "your_openai_api_key"
if not os.getenv("OPENAI_API_KEY"):
    logging.warning("OPENAI_API_KEY environment variable not set.")
    # exit(1)

# Elasticsearch - Environment variables are now primarily handled *inside* ElasticsearchUtils
# We still need the index name here.
# Common Env Vars for the new util: ELASTICSEARCH_HOST, ELASTICSEARCH_CLOUD_ID, ELASTICSEARCH_API_KEY, ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD
ARTICLE_INDEX_NAME = os.getenv(
    "ARTICLE_INDEX_NAME", "news_data"
)  # Get index name from env or use default
PRICE_TABLE_NAME = "stock_prices"

# --- Configuration ---
# Environment for Elasticsearch
ELASTICSEARCH_HOST = os.environ.get("ELASTICSEARCH_HOST", "localhost")
ELASTICSEARCH_PORT = int(os.environ.get("ELASTICSEARCH_PORT", 9200))
ELASTICSEARCH_PASSWORD = os.environ.get("ELASTICSEARCH_PASSWORD")

# Environment for postgresdb
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "postgres")  # Choose your DB name
DB_USER = os.environ.get("DB_USER", "postgres")  # Choose your DB user
DB_PASS = os.environ.get("DB_PASS", "postgres")  # Choose your DB password

db_params = {
    "host": DB_HOST,
    "port": DB_PORT,  # Default port if not set
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS
}

# Global utility instances (initialize later in main block)
pg_utils = PostgresUtils(**db_params)
es_utils = ElasticsearchUtils()

# --- Initialize LLM ---
llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.2)