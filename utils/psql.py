import io
import logging

import pandas as pd
import psycopg2
from psycopg2 import Error as Psycopg2Error
from psycopg2 import OperationalError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PostgresUtils:
    """
    A utility class for interacting with a PostgreSQL database,
    including fetching data into Pandas DataFrames and pushing
    DataFrames into tables.
    """

    def __init__(self, dbname, user, password, host='localhost', port=5432):
        """
        Initializes the PostgresUtils instance and establishes connections.

        Args:
            dbname (str): The name of the database.
            user (str): The username for database authentication.
            password (str): The password for database authentication.
            host (str): The database host address. Defaults to 'localhost'.
            port (int): The database port. Defaults to 5432.
        """
        self.db_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.connection = None
        self.cursor = None
        self.engine = None  # SQLAlchemy engine for pandas.to_sql
        self._connect()
    
    def _connect(self):
        """Establishes the psycopg2 connection and cursor, and SQLAlchemy engine."""
        try:
            # Psycopg2 connection
            logging.info(f"Attempting to connect to Postgres DB '{self.db_params['dbname']}' on {self.db_params['host']}:{self.db_params['port']}...")
            self.connection = psycopg2.connect(**self.db_params)
            self.cursor = self.connection.cursor()
            logging.info("Psycopg2 connection successful.")

            # SQLAlchemy engine (uses psycopg2 driver)
            # Format: postgresql+psycopg2://user:password@host:port/dbname
            engine_url = (
                f"postgresql+psycopg2://{self.db_params['user']}:{self.db_params['password']}"
                f"@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['dbname']}"
            )
            self.engine = create_engine(engine_url)
            # Test SQLAlchemy connection (optional but good practice)
            with self.engine.connect() as _:
                logging.info("SQLAlchemy engine created and connection tested successfully.")

        except (OperationalError, Psycopg2Error, SQLAlchemyError) as e:
            logging.error(f"Database connection failed: {e}")
            # Clean up partial connections if any
            if self.cursor: self.cursor.close()
            if self.connection: self.connection.close()
            if self.engine: self.engine.dispose()
            self.connection = None
            self.cursor = None
            self.engine = None
            raise  # Re-raise the exception after logging

    def get_data_as_dataframe(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Executes a SQL query and returns the results as a Pandas DataFrame.

        Args:
            query (str): The SQL query string to execute. Use placeholders (%s)
                         for parameters to prevent SQL injection.
            params (tuple, optional): A tuple of parameters to substitute into
                                      the query placeholders. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
                          Returns an empty DataFrame if no data is found or
                          if the connection is not established.

        Raises:
            Psycopg2Error: If there's an error during query execution.
        """
        if not self.connection or not self.cursor:
            logging.error("No active database connection.")
            return pd.DataFrame() # Return empty DataFrame if not connected

        try:
            logging.info(f"Executing query: {query}" + (f" with params: {params}" if params else ""))
            self.cursor.execute(query, params or ()) # Pass params safely
            column_names = [desc[0] for desc in self.cursor.description]
            data = self.cursor.fetchall()
            logging.info(f"Query executed successfully, fetched {len(data)} rows.")
            return pd.DataFrame(data, columns=column_names)
        except Psycopg2Error as e:
            logging.error(f"Error executing query '{query}': {e}")
            self.connection.rollback() # Rollback transaction on error
            raise # Re-raise the specific psycopg2 error

    def push_dataframe_to_table(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', index: bool = False, chunksize: int = None):
        """
        Pushes data from a Pandas DataFrame to a specified PostgreSQL table.
        Uses SQLAlchemy's engine for efficient insertion.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to insert.
            table_name (str): The name of the target table in the database.
            if_exists (str): How to behave if the table already exists.
                             Options: 'fail', 'replace', 'append'. Defaults to 'append'.
            index (bool): Write DataFrame index as a column. Defaults to False.
            chunksize (int, optional): Rows to insert per batch. If None, all rows
                                      are inserted at once. Defaults to None. Recommended
                                      for large DataFrames (e.g., 1000 or 10000).

        Raises:
            SQLAlchemyError: If there's an error during the DataFrame insertion.
            ValueError: If the DataFrame is empty or connection is not available.
        """
        if not self.engine:
            logging.error("No active SQLAlchemy engine. Cannot push DataFrame.")
            raise ValueError("Database engine not initialized.")

        if df.empty:
            logging.warning(f"DataFrame is empty. No data pushed to table '{table_name}'.")
            return # Or raise ValueError("Cannot push an empty DataFrame.")

        try:
            logging.info(f"Pushing {len(df)} rows to table '{table_name}' (if_exists='{if_exists}', chunksize={chunksize})...")
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
                method='multi' # Often faster for psycopg2, inserts multiple rows in one statement
            )
            logging.info(f"Successfully pushed DataFrame to table '{table_name}'.")
        except (SQLAlchemyError, ValueError) as e: # ValueError can be raised by to_sql for bad inputs
            logging.error(f"Error pushing DataFrame to table '{table_name}': {e}")
            # Note: pandas.to_sql handles its own transactions with SQLAlchemy engine
            # No explicit rollback needed here unless you wrap it in a broader psycopg2 transaction
            raise

    def execute_query(self, query: str, params: tuple = None):
        """
        Executes a query that doesn't return data (e.g., INSERT, UPDATE, DELETE, CREATE).

        Args:
            query (str): The SQL query string to execute. Use placeholders (%s).
            params (tuple, optional): A tuple of parameters for the query.

        Raises:
            Psycopg2Error: If there's an error during query execution.
        """
        if not self.connection or not self.cursor:
            logging.error("No active database connection.")
            raise ConnectionError("Database connection not established.")

        try:
            logging.info(f"Executing action query: {query}" + (f" with params: {params}" if params else ""))
            self.cursor.execute(query, params or ())
            self.connection.commit() # Commit changes for non-SELECT queries
            logging.info("Action query executed and committed successfully.")
        except Psycopg2Error as e:
            logging.error(f"Error executing action query '{query}': {e}")
            self.connection.rollback() # Rollback transaction on error
            raise

    def close(self):
        """Closes the cursor, connection, and disposes the engine."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
            logging.info("Psycopg2 cursor closed.")
        if self.connection:
            self.connection.close()
            self.connection = None
            logging.info("Psycopg2 connection closed.")
        if self.engine:
            self.engine.dispose() # Releases connection pool resources
            self.engine = None
            logging.info("SQLAlchemy engine disposed.")

    def __enter__(self):
        """Enter the runtime context related to this object."""
        # Re-establish connection if it was closed or failed previously
        if not self.connection or self.connection.closed != 0:
             self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        self.close()