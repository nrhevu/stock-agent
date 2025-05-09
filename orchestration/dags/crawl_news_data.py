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
    dag_id="crawl_news_data",
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
            "docker run --rm -v /Users/rhevu/Works/Projects/stock-agent/:/app -w /app nrhevu/stock-runner:v1.0 mkdir -p data/news_data/$day"
        ),
    )

    # run crawler
    crawl_data_nvidia = BashOperator(
        task_id="crawl_data_nvidia",
        bash_command=(
            "day=\"{{ ti.xcom_pull(task_ids='get_date') }}\" && "
            'echo "Running crawl nvidia data day: $day" && '
            'docker run --rm -v /Users/rhevu/Works/Projects/stock-agent/:/app -w /app/data_ingestion/new_crawlers/ nrhevu/stock-runner:v1.0 scrapy crawl crawler -a keyword="nvidia" -o /app/data/news_data/$day/nvidia.json'
        ),
    )

    crawl_data_google = BashOperator(
        task_id="crawl_data_google",
        bash_command=(
            "day=\"{{ ti.xcom_pull(task_ids='get_date') }}\" && "
            'echo "Running crawl google data for day: $day" && '
            'docker run --rm -v /Users/rhevu/Works/Projects/stock-agent/:/app -w /app/data_ingestion/new_crawlers/ nrhevu/stock-runner:v1.0 scrapy crawl crawler -a keyword="google" -o /app/data/news_data/$day/google.json'
        ),
    )

    crawl_data_microsoft = BashOperator(
        task_id="crawl_data_microsoft",
        bash_command=(
            "day=\"{{ ti.xcom_pull(task_ids='get_date') }}\" && "
            'echo "Running crawl microsoft data for day: $day" && '
            'docker run --rm -v /Users/rhevu/Works/Projects/stock-agent/:/app -w /app/data_ingestion/new_crawlers/ nrhevu/stock-runner:v1.0 scrapy crawl crawler -a keyword="microsoft" -o /app/data/news_data/$day/microsoft.json'
        ),
    )

    process_data_elk = BashOperator(
        task_id="process_data_elk",
        bash_command=(
            "day=\"{{ ti.xcom_pull(task_ids='get_date') }}\" && "
            'echo "Running process data for day: $day" && '
            "docker run --rm -v /Users/rhevu/Works/Projects/stock-agent/:/app -w /app --network=stock-agent_default -e ELASTICSEARCH_HOST=http://elasticsearch:9200 nrhevu/stock-runner:v1.0 python3 process_news_data.py --data-dir /app/data/news_data/$day/"
        ),
    )

    # Task dependency
    (
        get_date
        >> create_folder
        >> [crawl_data_nvidia, crawl_data_google, crawl_data_microsoft]
        >> process_data_elk
    )
