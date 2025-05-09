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

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Database Configuration (via environment variables) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")

# --- Data & Table Configuration ---
STOCK_DATA_DIR = "data/stock_data"  # Relative path to your stock data folder
TABLE_NAME = "stock_prices"         # Name for the target Postgres table

# Columns expected to exist **in the incoming CSV files**
REQUIRED_COLUMNS = [
    "Ticker",  # New format explicitly contains ticker per row
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
]

# Target database columns (follow lowercase naming convention)
TARGET_DB_COLUMNS = [
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def create_stock_table(pg_utils, table_name):
    """Create the stock_prices table if it does not already exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        ticker VARCHAR(10) NOT NULL,
        date   DATE        NOT NULL,
        open   NUMERIC(15, 6),
        high   NUMERIC(15, 6),
        low    NUMERIC(15, 6),
        close  NUMERIC(15, 6),
        volume BIGINT,
        PRIMARY KEY (ticker, date)
    );
    """
    try:
        logging.info(f"Ensuring table '{table_name}' exists …")
        pg_utils.execute_query(create_table_sql)
        logging.info(f"Table '{table_name}' is ready.")
    except Psycopg2Error as e:
        logging.error(f"Failed to create or verify table '{table_name}': {e}")
        raise  # Stop the script if table creation fails


def process_stock_file(filepath: str, fallback_ticker: str | None = None) -> pd.DataFrame | None:
    """Read and clean a single CSV file produced in the *new* format.

    The new format looks like:

        ,Ticker,Date,Close,High,Low,Open,Volume,Target
        0,GOOGL,2004-08-19, …

    The first unnamed column is an export-created index — it will be dropped.
    The trailing ``Target`` column is not needed for the price table and will
    likewise be removed.
    """

    logging.info(f"Processing file: {filepath}")

    try:
        # Read with header; do *not* skip rows – new format already has a header
        df = pd.read_csv(filepath)

        # ------------------------------------------------------------------
        # 1️⃣  Drop any automatically‑generated index column (usually "Unnamed: 0")
        # ------------------------------------------------------------------
        unnamed_cols = [c for c in df.columns if c.lower().startswith("unnamed") or c == ""]
        if unnamed_cols:
            df.drop(columns=unnamed_cols, inplace=True)

        # ------------------------------------------------------------------
        # 2️⃣  Validate required columns
        # ------------------------------------------------------------------
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required column(s): {', '.join(missing)}")

        # ------------------------------------------------------------------
        # 3️⃣  Ensure ticker column is present / standardised
        # ------------------------------------------------------------------
        if df["Ticker"].isna().all():
            if not fallback_ticker:
                raise ValueError("Ticker column is empty and no fallback_ticker supplied.")
            df["Ticker"] = fallback_ticker.upper()
        else:
            df["Ticker"] = df["Ticker"].str.upper().fillna(fallback_ticker or "")

        # ------------------------------------------------------------------
        # 4️⃣  Remove columns we do not store (e.g. "Target")
        # ------------------------------------------------------------------
        df.drop(columns=[c for c in df.columns if c not in REQUIRED_COLUMNS], inplace=True, errors="ignore")

        # ------------------------------------------------------------------
        # 5️⃣  Type conversions
        # ------------------------------------------------------------------
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

        # ------------------------------------------------------------------
        # 6️⃣  Drop rows with critical NA values
        # ------------------------------------------------------------------
        before = len(df)
        df.dropna(subset=["Date"] + numeric_cols, inplace=True)
        dropped = before - len(df)
        if dropped:
            logging.warning(f"Dropped {dropped} incomplete rows from {os.path.basename(filepath)}")

        if df.empty:
            logging.warning(f"No usable data in {filepath} after cleaning; skipping file.")
            return None

        # ------------------------------------------------------------------
        # 7️⃣  Rename columns to DB‑friendly names & final ordering
        # ------------------------------------------------------------------
        df.rename(
            columns={
                "Ticker": "ticker",
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            },
            inplace=True,
        )

        # Keep only the columns we actually insert and ensure correct order
        df = df[TARGET_DB_COLUMNS]

        # Ensure 'date' is pure date (not datetime)
        df["date"] = pd.to_datetime(df["date"]).dt.date

        return df

    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
        return None
    except pd.errors.EmptyDataError:
        logging.warning(f"File is empty: {filepath}")
        return None
    except Exception as e:
        logging.error("Error processing file %s: %s", filepath, e, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Main Orchestration Function
# ---------------------------------------------------------------------------

def main():
    logging.info("Starting stock‑data ingestion process …")

    if not os.path.isdir(STOCK_DATA_DIR):
        logging.error(f"Stock data directory not found: {STOCK_DATA_DIR}")
        return

    files_processed = 0
    files_failed = 0

    try:
        # Use PostgresUtils as a context manager for automatic connection handling
        with PostgresUtils(DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT) as pg:
            logging.info("Database connection established.")

            # Ensure target table exists
            create_stock_table(pg, TABLE_NAME)

            # Iterate through each CSV file in the directory
            for filename in os.listdir(STOCK_DATA_DIR):
                if not filename.lower().endswith(".csv"):
                    continue  # Skip non‑CSV files

                filepath = os.path.join(STOCK_DATA_DIR, filename)

                # Fallback ticker extracted from filename (e.g. "AAPL" in "AAPL_data.csv")
                fallback_ticker = (filename.split("_")[0] or "").upper()

                # Process file
                df_clean = process_stock_file(filepath, fallback_ticker=fallback_ticker)

                if df_clean is None or df_clean.empty:
                    files_failed += 1
                    continue

                # Push to database
                try:
                    logging.info(
                        f"Inserting {len(df_clean)} rows from {filename} into '{TABLE_NAME}' …"
                    )
                    pg.push_dataframe_to_table(
                        df=df_clean,
                        table_name=TABLE_NAME,
                        if_exists="append",
                        index=False,
                        chunksize=1000,
                    )
                    files_processed += 1
                except Exception as push_err:
                    logging.error(f"Failed to push data from {filename}: {push_err}")
                    files_failed += 1

    except (ImportError, OperationalError, Psycopg2Error) as db_err:
        logging.error("Database‑related error: %s", db_err, exc_info=True)
    except Exception as err:
        logging.error("Unexpected error: %s", err, exc_info=True)
    finally:
        logging.info("Ingestion finished – success: %s | failed: %s", files_processed, files_failed)


if __name__ == "__main__":
    main()
