"""
DAG: bash_command_with_date
Runs a Bash command after capturing today's date.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── Default task args ──────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "start_date": datetime(2025, 5, 9),
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="crawl_stock_data",
    description="Crawl news data for specific companies",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "bash", "xcom"],
) as dag:

    # 1️⃣ Python task → push a date string to XCom
    def get_today_date(**context):
        """
        Return today's date in YYYY-MM-DD format.
        Whatever you `return` becomes the XCom value.
        """
        today_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        return today_str

    get_date = PythonOperator(
        task_id="get_date",
        python_callable=get_today_date,
    )

    # 2️⃣ create a folder using the date from XCom
    create_folder = BashOperator(
        task_id="create_folder",
        bash_command=(
            "day=\"{{ ti.xcom_pull(task_ids='get_date') }}\" && "
            'echo "Running job for day: $day" && '
            # place your real command below; $day is available
            "docker run --rm -v $WORKSPACE_DIR:/app -w /app nrhevu/stock-runner:v1.0 mkdir -p data/stock_data/$day"
        ),
    )

    # run crawler
    crawl_stock_data = BashOperator(
        task_id="crawl_stock_data",
        bash_command=(
            "day=\"{{ ti.xcom_pull(task_ids='get_date') }}\" && "
            'echo "Running crawl stock data day: $day" && '
            'docker run --rm -v $WORKSPACE_DIR:/app nrhevu/stock-runner:v1.0 python data_ingestion/stock_api.py --save_dir /app/data/stock_data/$day/'
        ),
    )


    process_data_postgres = BashOperator(
        task_id="process_data_postgres",
        bash_command=(
            "day=\"{{ ti.xcom_pull(task_ids='get_date') }}\" && "
            'echo "Running process data for day: $day" && '
            "docker run --rm -v $WORKSPACE_DIR:/app -w /app --network=stock-agent_default -e DB_HOST=postgres nrhevu/stock-runner:v1.0 python process_stock_data.py --data-dir /app/data/stock_data/$day/"
        ),
    )

    # Task dependency
    (
        get_date
        >> create_folder
        >> crawl_stock_data
        >> process_data_postgres
    )
