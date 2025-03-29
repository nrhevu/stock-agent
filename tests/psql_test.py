import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

import os

import pandas as pd
import pandas.testing as pd_testing
import pytest
from dotenv import load_dotenv
from psycopg2 import Error as Psycopg2Error
from psycopg2 import OperationalError
from sqlalchemy.exc import \
    SQLAlchemyError  # ProgrammingError might be raised by pandas/sqlalchemy on table exists with if_exists='fail'

load_dotenv()

# --- Assuming PostgresUtils is in utils/postgres_utils.py ---
# Adjust the import path based on your project structure
try:
    from utils.psql import PostgresUtils
except ImportError:
    # Add fallback logic or skip tests if the module can't be imported
    pytest.skip(
        "Skipping Postgres tests: Cannot import PostgresUtils", allow_module_level=True
    )


# --- Configuration ---
# Read connection details from environment variables
# Skip all tests in this module if any variable is missing
db_params = {
    "host": os.environ.get("DB_HOST"),
    "port": int(os.environ.get("DB_PORT", 5432)),  # Default port if not set
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
}
@pytest.fixture(scope="function")
def get_db_params():
    """Fixture to provide database connection parameters."""
    return db_params


# Check if all necessary environment variables are set
missing_vars = [
    k for k, v in db_params.items() if v is None and k != "port"
]  # port has default

# Skip the entire module if connection details are incomplete
pytestmark = pytest.mark.skipif(
    bool(missing_vars),
    reason=f"Missing test DB environment variables: {', '.join(missing_vars)}. Skipping Postgres tests.",
)

# Define a test table name
TEST_TABLE = "pytest_temp_test_table"

# --- Fixtures ---


@pytest.fixture(scope="function")  # Run for each test function
def pg_utils():
    """Fixture to create and tear down PostgresUtils instance."""
    utils = None
    try:
        utils = PostgresUtils(**db_params)
        # Ensure the test table doesn't exist before the test
        try:
            utils.execute_query(f"DROP TABLE IF EXISTS {TEST_TABLE};")
        except Psycopg2Error as drop_err:
            # Ignore errors if table doesn't exist, but log others
            print(f"Note: Pre-test DROP TABLE failed (might be okay): {drop_err}")
            utils.connection.rollback()  # Rollback if drop failed mid-transaction

        yield utils  # Provide the instance to the test function

    except (OperationalError, Psycopg2Error) as e:
        pytest.fail(f"Failed to connect to the test database: {e}")
    finally:
        if utils:
            # Clean up: Drop the test table after the test runs
            try:
                if utils.connection and not utils.connection.closed:
                    utils.execute_query(f"DROP TABLE IF EXISTS {TEST_TABLE};")
            except Psycopg2Error as drop_err:
                print(f"Warning: Post-test DROP TABLE failed: {drop_err}")
                if utils.connection and not utils.connection.closed:
                    utils.connection.rollback()
            finally:
                utils.close()  # Ensure connection is closed


# --- Test Functions ---


def test_connection_success(pg_utils):
    """Test if the connection and engine objects are created."""
    assert pg_utils.connection is not None, "Psycopg2 connection should be established"
    assert pg_utils.cursor is not None, "Psycopg2 cursor should be established"
    assert pg_utils.engine is not None, "SQLAlchemy engine should be established"
    assert pg_utils.connection.closed == 0, "Psycopg2 connection should be open"


def test_execute_query_create_table(pg_utils):
    """Test creating a table using execute_query."""
    create_sql = f"""
    CREATE TABLE {TEST_TABLE} (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50),
        value INTEGER
    );
    """
    try:
        pg_utils.execute_query(create_sql)
        # Verify table exists by trying to insert (or query information_schema)
        pg_utils.execute_query(
            f"INSERT INTO {TEST_TABLE} (name, value) VALUES (%s, %s);", ("test", 1)
        )
        count_df = pg_utils.get_data_as_dataframe(f"SELECT COUNT(*) FROM {TEST_TABLE};")
        assert count_df.iloc[0, 0] == 1
    except Psycopg2Error as e:
        pytest.fail(f"execute_query for CREATE TABLE failed: {e}")


def test_push_and_get_dataframe(pg_utils):
    """Test pushing data via DataFrame and retrieving it."""
    # 1. Create the table first
    create_sql = f"CREATE TABLE {TEST_TABLE} (ticker VARCHAR(10), price REAL);"
    pg_utils.execute_query(create_sql)

    # 2. Prepare DataFrame
    data = {"ticker": ["AAA", "BBB", "CCC"], "price": [10.5, 22.1, 35.0]}
    df_to_push = pd.DataFrame(data)

    # 3. Push DataFrame
    try:
        pg_utils.push_dataframe_to_table(
            df_to_push, TEST_TABLE, if_exists="append", index=False
        )
    except (SQLAlchemyError, ValueError, Psycopg2Error) as e:
        pytest.fail(f"push_dataframe_to_table failed: {e}")

    # 4. Get DataFrame back
    try:
        df_retrieved = pg_utils.get_data_as_dataframe(
            f"SELECT ticker, price FROM {TEST_TABLE} ORDER BY ticker;"
        )
    except Psycopg2Error as e:
        pytest.fail(f"get_data_as_dataframe failed: {e}")

    # 5. Verify
    assert not df_retrieved.empty, "Retrieved DataFrame should not be empty"
    assert len(df_retrieved) == len(df_to_push), "Retrieved DataFrame length mismatch"
    # Ensure column order doesn't matter for comparison, sort columns if needed
    df_to_push_sorted = df_to_push.sort_values(by="ticker").reset_index(drop=True)
    df_retrieved_sorted = df_retrieved.sort_values(by="ticker").reset_index(drop=True)
    pd_testing.assert_frame_equal(
        df_retrieved_sorted, df_to_push_sorted, check_dtype=False
    )  # Allow float type differences (REAL vs float64)


def test_push_dataframe_if_exists_replace(pg_utils):
    """Test the if_exists='replace' option."""
    # 1. Create table and push initial data
    pg_utils.execute_query(f"CREATE TABLE {TEST_TABLE} (id INT, name VARCHAR(10));")
    df1 = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    pg_utils.push_dataframe_to_table(df1, TEST_TABLE, if_exists="append", index=False)

    # 2. Prepare new data and push with 'replace'
    df2 = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})
    pg_utils.push_dataframe_to_table(df2, TEST_TABLE, if_exists="replace", index=False)

    # 3. Get data and verify only the second DataFrame exists
    df_retrieved = pg_utils.get_data_as_dataframe(
        f"SELECT id, name FROM {TEST_TABLE} ORDER BY id;"
    )
    df2_sorted = df2.sort_values(by="id").reset_index(drop=True)
    df_retrieved_sorted = df_retrieved.sort_values(by="id").reset_index(drop=True)
    pd_testing.assert_frame_equal(df_retrieved_sorted, df2_sorted)


def test_push_dataframe_if_exists_fail(pg_utils):
    """Test the if_exists='fail' option."""
    # 1. Create table and push initial data
    pg_utils.execute_query(f"CREATE TABLE {TEST_TABLE} (id INT);")
    df1 = pd.DataFrame({"id": [1]})
    pg_utils.push_dataframe_to_table(df1, TEST_TABLE, if_exists="append", index=False)

    # 2. Try pushing again with 'fail' - should raise an error
    df2 = pd.DataFrame({"id": [2]})
    # Pandas/SQLAlchemy might raise ValueError or a db-specific error like ProgrammingError
    with pytest.raises((ValueError, SQLAlchemyError, Psycopg2Error)):
        pg_utils.push_dataframe_to_table(df2, TEST_TABLE, if_exists="fail", index=False)


def test_get_data_with_parameters(pg_utils):
    """Test fetching data using parameterized query."""
    # 1. Setup table and data
    pg_utils.execute_query(f"CREATE TABLE {TEST_TABLE} (name VARCHAR(10), value INT);")
    pg_utils.execute_query(
        f"INSERT INTO {TEST_TABLE} (name, value) VALUES (%s, %s), (%s, %s);",
        ("X", 10, "Y", 25),
    )

    # 2. Fetch subset using parameters
    query = f"SELECT name, value FROM {TEST_TABLE} WHERE value > %s ORDER BY value;"
    params = (15,)
    df_retrieved = pg_utils.get_data_as_dataframe(query, params)

    # 3. Verify
    expected_df = pd.DataFrame({"name": ["Y"], "value": [25]})
    pd_testing.assert_frame_equal(df_retrieved.reset_index(drop=True), expected_df)


def test_get_data_no_results(pg_utils):
    """Test fetching data when the query returns no rows."""
    pg_utils.execute_query(f"CREATE TABLE {TEST_TABLE} (id INT);")  # Create empty table
    df_retrieved = pg_utils.get_data_as_dataframe(f"SELECT * FROM {TEST_TABLE};")
    assert df_retrieved.empty, "DataFrame should be empty when query returns no results"
    assert list(df_retrieved.columns) == [
        "id"
    ], "DataFrame should still have columns even if empty"  # Check columns are derived


def test_push_empty_dataframe(pg_utils):
    """Test pushing an empty DataFrame (should not fail)."""
    pg_utils.execute_query(f"CREATE TABLE {TEST_TABLE} (id INT);")
    empty_df = pd.DataFrame(
        {"id": pd.Series(dtype="int")}
    )  # Create empty DF with correct dtype
    try:
        pg_utils.push_dataframe_to_table(empty_df, TEST_TABLE, if_exists="append")
        # Verify table is still empty
        count_df = pg_utils.get_data_as_dataframe(f"SELECT COUNT(*) FROM {TEST_TABLE};")
        assert count_df.iloc[0, 0] == 0
    except Exception as e:
        pytest.fail(f"Pushing an empty DataFrame failed unexpectedly: {e}")


def test_context_manager(get_db_params):
    """Test using the class as a context manager."""
    utils_instance = None
    db_params = get_db_params
    try:
        with PostgresUtils(**db_params) as utils:
            utils_instance = utils
            # Perform a simple action
            utils.execute_query("SELECT 1;")
            assert (
                utils.connection.closed == 0
            ), "Connection should be open inside 'with' block"
        # After exiting the 'with' block
        assert utils_instance is not None, "Utils instance should have been created"
        # Note: Checking utils_instance.connection.closed might be unreliable
        # if the connection object itself is None after close().
        # Instead, try using it and expect an error.
        with pytest.raises(
            Exception
        ):  # Expect psycopg2.InterfaceError or AttributeError
            if utils_instance.connection:  # Check if connection object exists first
                utils_instance.connection.cursor()  # Try using the closed connection
            else:
                raise AttributeError(
                    "Connection object is None"
                )  # Treat None as closed

    except (OperationalError, Psycopg2Error) as e:
        pytest.fail(f"Context manager test failed during connection or query: {e}")
