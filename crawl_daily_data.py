#!/usr/bin/env python3
"""
Script to crawl daily news and stock data.
This script is designed to be run from a Docker container with the entire project mounted.
It uses Scrapy for news crawling and yfinance for stock data crawling.
Data is processed and uploaded to Elasticsearch (news) and PostgreSQL (stock).
"""

import json
import logging
import os
import sys
import hashlib
from datetime import datetime
from glob import glob
from typing import Dict, List, Set, Any

import pandas as pd
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "crawl_daily_data.log"))
    ]
)
logger = logging.getLogger(__name__)

# Ensure necessary directories exist
def ensure_dirs_exist():
    """Ensure all necessary directories exist."""
    dirs = [
        "data/news_data",
        "data/stock_data"
    ]
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Ensured directory exists: {dir_path}")

# News Crawling Functions
def crawl_news_data():
    """Crawl news data for all companies using Scrapy."""
    logger.info("Starting news data crawling...")
    
    # Define the companies to crawl
    companies = ['nvidia', 'google', 'microsoft']
    
    # Get the current date for the output filename
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Path to the crawler directory
    crawler_dir = os.path.join(os.path.dirname(__file__), "data_ingestion", "new_crawlers")
    
    # Output directory
    output_dir = os.path.join(os.path.dirname(__file__), "data", "news_data")
    
    # Crawl news for each company
    for company in companies:
        output_file = os.path.join(output_dir, f"{company}_{today}.json")
        
        # Build the command to run the Scrapy crawler
        cmd = f"cd {crawler_dir} && scrapy crawl crawler -a keyword={company} -o {output_file} -t json"
        
        logger.info(f"Running command: {cmd}")
        exit_code = os.system(cmd)
        
        if exit_code == 0:
            logger.info(f"Successfully crawled news for {company}")
            
            # Check if the file was created and has content
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                try:
                    # Filter duplicates
                    filter_duplicate_news(output_file)
                except Exception as e:
                    logger.error(f"Error filtering duplicates for {company}: {e}")
            else:
                logger.warning(f"No data was crawled for {company} or file was not created")
        else:
            logger.error(f"Failed to crawl news for {company}, exit code: {exit_code}")
    
    logger.info("Completed news data crawling")

def filter_duplicate_news(json_path: str):
    """Filter duplicate news articles based on title and content hash."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            articles = json.load(f)
        
        logger.info(f"Filtering duplicates from {len(articles)} articles in {json_path}")
        
        # Use a set to track unique content hashes
        unique_hashes = set()
        unique_titles = set()
        filtered_articles = []
        
        for article in articles:
            # Create a hash of the content to identify duplicates
            content = article.get('content', '')
            title = article.get('title', '')
            
            if not title:
                continue  # Skip articles without a title
            
            # Create a hash of the content
            content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
            
            # Check if we've seen this title or content hash before
            if title not in unique_titles and content_hash not in unique_hashes:
                unique_titles.add(title)
                unique_hashes.add(content_hash)
                filtered_articles.append(article)
        
        # Write the filtered articles back to the file
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(filtered_articles, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Removed {len(articles) - len(filtered_articles)} duplicate articles. Kept {len(filtered_articles)} unique articles.")
    
    except Exception as e:
        logger.error(f"Error filtering duplicates in {json_path}: {e}", exc_info=True)
        raise

# Stock Crawling Functions
def crawl_stock_data():
    """Crawl stock data for all tickers using yfinance."""
    logger.info("Starting stock data crawling...")
    
    try:
        # Import the stock crawler
        sys.path.append(os.path.dirname(__file__))
        from data_ingestion.stock_crawler import crawl_monthly_stock
        
        # Define the tickers to crawl
        tickers = ['MSFT', 'NVDA', 'GOOGL']
        
        # Get the current date for the output filename
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Output directory
        output_dir = os.path.join(os.path.dirname(__file__), "data", "stock_data")
        
        # Crawl stock data
        results = crawl_monthly_stock(tickers=tickers, period="1mo")
        
        # Save each ticker's data to a CSV file
        for ticker, df in results.items():
            output_file = os.path.join(output_dir, f"{ticker}_{today}.csv")
            
            # Filter duplicates by date (index)
            df = df[~df.index.duplicated(keep='first')]
            
            # Save to CSV with proper formatting
            df.to_csv(output_file, index=True)
            logger.info(f"Saved stock data for {ticker} to {output_file} ({len(df)} rows)")
        
        logger.info(f"Successfully crawled stock data for {len(results)} tickers")
    
    except Exception as e:
        logger.error(f"Error crawling stock data: {e}", exc_info=True)
    
    logger.info("Completed stock data crawling")

# Process News Data
def process_news_data():
    """Process and index news data to Elasticsearch with duplicate filtering."""
    logger.info("Starting news data processing...")
    
    try:
        # Import the news data processor
        sys.path.append(os.path.dirname(__file__))
        from process_news_data import translate_and_index_to_elk
        from utils.es import ElasticsearchUtils
        
        # Get the current date for finding today's files
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Find all news data files from today
        news_files = glob(os.path.join(os.path.dirname(__file__), "data", "news_data", f"*_{today}.json"))
        
        if not news_files:
            logger.warning("No news files found for today")
            return
        
        # Initialize Elasticsearch connection for duplicate checking
        es_utils = ElasticsearchUtils()
        
        # Process each file
        for json_path in news_files:
            # Extract company name from filename
            company = os.path.basename(json_path).split("_")[0]
            logger.info(f"Processing {company} articles from {json_path}")
            
            # Load articles
            with open(json_path, 'r', encoding='utf-8') as f:
                articles = json.load(f)
            
            # Check for duplicates in Elasticsearch
            filtered_articles = filter_elasticsearch_duplicates(articles, company, es_utils)
            
            # If we have filtered articles, save them back to the file
            if len(filtered_articles) < len(articles):
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(filtered_articles, f, ensure_ascii=False, indent=2)
                logger.info(f"Removed {len(articles) - len(filtered_articles)} duplicate articles from Elasticsearch check")
            
            # Translate and index
            translate_and_index_to_elk("news_data", json_path, company)
            logger.info(f"Finished processing {company} articles")
    
    except Exception as e:
        logger.error(f"Error processing news data: {e}", exc_info=True)
    
    logger.info("Completed news data processing")

def filter_elasticsearch_duplicates(articles: List[Dict[str, Any]], company: str, es_utils) -> List[Dict[str, Any]]:
    """Filter out articles that already exist in Elasticsearch."""
    filtered_articles = []
    
    for article in articles:
        title = article.get('title')
        if not title:
            continue
        
        # Search for existing articles with the same title and company
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"title_vi": title}},
                        {"match": {"company": company}}
                    ]
                }
            }
        }
        
        results = es_utils.search(query, index_name="news_data", size=1)
        
        # If no results, this is a new article
        if not results:
            filtered_articles.append(article)
    
    logger.info(f"Found {len(articles) - len(filtered_articles)} duplicate articles in Elasticsearch")
    return filtered_articles

# Process Stock Data
def process_stock_data():
    """Process and store stock data in PostgreSQL with duplicate filtering."""
    logger.info("Starting stock data processing...")
    
    try:
        # Import the PostgreSQL utility
        sys.path.append(os.path.dirname(__file__))
        from utils.psql import PostgresUtils
        import os
        from dotenv import load_dotenv
        
        load_dotenv()  # Load environment variables
        
        # Get database connection parameters
        DB_HOST = os.environ.get("DB_HOST", "localhost")
        DB_PORT = int(os.environ.get("DB_PORT", 5432))
        DB_NAME = os.environ.get("DB_NAME", "postgres")
        DB_USER = os.environ.get("DB_USER", "postgres")
        DB_PASS = os.environ.get("DB_PASS", "postgres")
        
        # Define the stock data directory and target table name
        STOCK_DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "stock_data")
        TABLE_NAME = "stock_prices"
        
        # Get the current date for finding today's files
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Find all stock data files from today
        stock_files = glob(os.path.join(STOCK_DATA_DIR, f"*_{today}.csv"))
        
        if not stock_files:
            logger.warning("No stock files found for today")
            return
        
        # Connect to PostgreSQL
        with PostgresUtils(DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT) as pg_utils:
            # Create the stock prices table if it doesn't exist
            create_stock_table(pg_utils, TABLE_NAME)
            
            # Process each stock file
            for csv_path in stock_files:
                # Extract ticker from filename
                ticker = os.path.basename(csv_path).split("_")[0]
                
                # Process the file
                process_stock_file(csv_path, ticker, pg_utils, TABLE_NAME)
    
    except Exception as e:
        logger.error(f"Error processing stock data: {e}", exc_info=True)
    
    logger.info("Completed stock data processing")

def create_stock_table(pg_utils, table_name):
    """Creates the stock prices table if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        ticker VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open NUMERIC(15, 6),
        high NUMERIC(15, 6),
        low NUMERIC(15, 6),
        close NUMERIC(15, 6),
        volume BIGINT,
        target SMALLINT,
        PRIMARY KEY (ticker, date)
    );
    """
    try:
        logger.info(f"Ensuring table '{table_name}' exists...")
        pg_utils.execute_query(create_table_sql)
        logger.info(f"Table '{table_name}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify table '{table_name}': {e}")
        raise

def process_stock_file(filepath, ticker, pg_utils, table_name):
    """Process a stock CSV file and upload to PostgreSQL with duplicate filtering."""
    logger.info(f"Processing file: {filepath} for ticker: {ticker}")
    
    try:
        # Read the CSV file
        df = pd.read_csv(filepath)
        
        # Ensure the date column is properly formatted
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Add ticker column
        df['ticker'] = ticker.upper()
        
        # Rename columns to match database schema
        df.rename(
            columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Target': 'target'
            },
            inplace=True
        )
        
        # Ensure date is just a date (not timestamp)
        df['date'] = df['date'].dt.date
        
        # Check for existing records to avoid duplicates
        dates_str = ", ".join([f"'{d}'" for d in df['date'].astype(str).tolist()])
        check_query = f"""
        SELECT date FROM {table_name} 
        WHERE ticker = %s AND date IN ({dates_str})
        """
        
        existing_dates_df = pg_utils.get_data_as_dataframe(check_query, (ticker,))
        
        if not existing_dates_df.empty:
            existing_dates = set(existing_dates_df['date'].astype(str).tolist())
            logger.info(f"Found {len(existing_dates)} existing records for {ticker}")
            
            # Filter out existing dates
            df = df[~df['date'].astype(str).isin(existing_dates)]
        
        if df.empty:
            logger.info(f"No new data to insert for {ticker}")
            return
        
        # Select and reorder columns for insertion
        df_processed = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'target']]
        
        # Push to database
        logger.info(f"Pushing {len(df_processed)} rows for ticker {ticker} to table '{table_name}'...")
        pg_utils.push_dataframe_to_table(
            df=df_processed,
            table_name=table_name,
            if_exists="append",
            index=False,
            chunksize=1000
        )
        logger.info(f"Successfully pushed data for {ticker}.")
    
    except Exception as e:
        logger.error(f"Error processing file {filepath}: {e}", exc_info=True)

# Main function
def main():
    """Main function to orchestrate the data crawling and processing."""
    logger.info("Starting daily data crawling and processing...")
    
    # Ensure directories exist
    ensure_dirs_exist()
    
    # Crawl news data
    crawl_news_data()
    
    # Crawl stock data
    crawl_stock_data()
    
    # Process news data
    process_news_data()
    
    # Process stock data
    process_stock_data()
    
    logger.info("Completed daily data crawling and processing")

if __name__ == "__main__":
    main()
