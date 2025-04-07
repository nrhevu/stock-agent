import logging
import os

import pandas as pd
from dotenv import load_dotenv
from psycopg2 import Error as Psycopg2Error
from psycopg2 import OperationalError

try:
    from utils.psql import PostgresUtils
except ImportError as e:
    print(f"Error importing PostgresUtils: {e}")
    print("Make sure PostgresUtils is defined and the path is correct.")
    exit(1)  # Exit if the essential utility can't be imported

load_dotenv()  # Load environment variables from .env file if present
# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- !! IMPORTANT !! ---
# Use Environment Variables (Recommended)
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "postgres")  # Choose your DB name
DB_USER = os.environ.get("DB_USER", "postgres")  # Choose your DB user
DB_PASS = os.environ.get("DB_PASS", "postgres")  # Choose your DB password

# Define the stock data directory and target table name
STOCK_DATA_DIR = "data/stock_data"  # Relative path to your stock data folder
TABLE_NAME = "stock_prices"  # Name for the target Postgres table

# Define expected columns based on the data rows (after skipping headers)
# The first column in the data rows is the date.
EXPECTED_CSV_COLUMNS = ["Date", "Close", "High", "Low", "Open", "Volume"]

# Define target database columns (lowercase convention)
TARGET_DB_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "volume"]


def create_stock_table(pg_utils, table_name):
    """Creates the stock prices table if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        ticker VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open NUMERIC(15, 6),  -- Adjust precision as needed
        high NUMERIC(15, 6),
        low NUMERIC(15, 6),
        close NUMERIC(15, 6),
        volume BIGINT,
        PRIMARY KEY (ticker, date) -- Composite primary key
    );
    """
    try:
        logging.info(f"Ensuring table '{table_name}' exists...")
        pg_utils.execute_query(create_table_sql)
        logging.info(f"Table '{table_name}' is ready.")
    except Psycopg2Error as e:
        logging.error(f"Failed to create or verify table '{table_name}': {e}")
        raise  # Re-raise the error to stop the script if table creation fails


def process_stock_file(filepath, ticker):
    """Reads and processes a single stock CSV file."""
    logging.info(f"Processing file: {filepath} for ticker: {ticker}")
    try:
        # Read CSV, skip the first 3 header rows. Assign names based on data order.
        df = pd.read_csv(filepath, skiprows=3, header=None, names=EXPECTED_CSV_COLUMNS)

        if df.empty:
            logging.warning(f"File {filepath} is empty or has no data rows.")
            return None

        # 1. Add Ticker column
        df["ticker"] = ticker.upper()  # Standardize ticker to uppercase

        # 2. Convert 'Date' column to datetime objects
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")  # Coerce errors to NaT

        # 3. Convert numeric columns
        numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")  # Coerce errors to NaN

        # 4. Handle missing values (NaT in Date, NaN in numerics)
        #    Drop rows where critical info (Date, Close) is missing
        initial_rows = len(df)
        df.dropna(subset=["Date"] + numeric_cols, inplace=True)
        if len(df) < initial_rows:
            logging.warning(
                f"Dropped {initial_rows - len(df)} rows with missing values from {filepath}"
            )

        if df.empty:
            logging.warning(f"DataFrame became empty after cleaning {filepath}")
            return None

        # 5. Rename columns to match database schema (lowercase)
        df.rename(
            columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                # 'ticker' is already correct
            },
            inplace=True,
        )

        # 6. Ensure 'date' column is just date (not timestamp) if needed
        df["date"] = df["date"].dt.date

        # 7. Select and reorder columns for insertion
        df_processed = df[TARGET_DB_COLUMNS]

        return df_processed

    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
        return None
    except pd.errors.EmptyDataError:
        logging.warning(f"File is empty: {filepath}")
        return None
    except Exception as e:
        logging.error(
            f"Error processing file {filepath}: {e}", exc_info=True
        )  # Log traceback
        return None


def main():
    """Main function to orchestrate the data ingestion."""
    logging.info("Starting stock data ingestion process...")

    # Check if data directory exists
    if not os.path.isdir(STOCK_DATA_DIR):
        logging.error(f"Stock data directory not found: {STOCK_DATA_DIR}")
        return  # Exit if data source is missing

    files_processed = 0
    files_failed = 0

    try:
        # Use context manager for automatic connection handling
        with PostgresUtils(DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT) as pg_utils:
            logging.info("Database connection established.")

            # Create the target table if it doesn't exist
            create_stock_table(pg_utils, TABLE_NAME)

            # Iterate through files in the stock data directory
            for filename in os.listdir(STOCK_DATA_DIR):
                if filename.lower().endswith(".csv"):
                    filepath = os.path.join(STOCK_DATA_DIR, filename)

                    # Extract ticker from filename (e.g., "MSFT" from "MSFT_1year_monthly.csv")
                    try:
                        ticker = filename.split("_")[0]
                        if not ticker:  # Handle cases like "_something.csv"
                            raise ValueError("Could not extract ticker")
                    except Exception:
                        logging.warning(
                            f"Could not extract ticker from filename: {filename}. Skipping."
                        )
                        files_failed += 1
                        continue

                    # Process the file into a DataFrame
                    processed_df = process_stock_file(filepath, ticker)

                    # Push the DataFrame to the database
                    if processed_df is not None and not processed_df.empty:
                        try:
                            logging.info(
                                f"Pushing {len(processed_df)} rows for ticker {ticker} to table '{TABLE_NAME}'..."
                            )
                            pg_utils.push_dataframe_to_table(
                                df=processed_df,
                                table_name=TABLE_NAME,
                                if_exists="append",  # Append data
                                index=False,
                                chunksize=1000,  # Use chunking for potentially large files
                            )
                            logging.info(f"Successfully pushed data for {ticker}.")
                            files_processed += 1
                        except Exception as push_error:  # Catch errors during push
                            logging.error(
                                f"Failed to push data for {ticker} from {filename}: {push_error}"
                            )
                            files_failed += 1
                            # Decide if you want to continue with other files or stop
                            # continue
                    elif processed_df is None:
                        files_failed += 1
                    else:  # Empty dataframe after processing
                        logging.info(
                            f"Skipping push for {ticker} as processed data was empty."
                        )

    except (ImportError, OperationalError, Psycopg2Error, Exception) as e:
        logging.error(
            f"An error occurred during the ingestion process: {e}", exc_info=True
        )  # Log traceback
        # Ensure connection is closed if error happens outside the 'with' block or during init
        # The 'with' block handles closing if init was successful

    finally:
        logging.info("Stock data ingestion process finished.")
        logging.info(f"Files processed successfully: {files_processed}")
        logging.info(f"Files failed or skipped: {files_failed}")


if __name__ == "__main__":
    main()
