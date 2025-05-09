"""
Example DAG demonstrating the usage of DockerOperator
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from docker.types import Mount

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'docker_example',
    default_args=default_args,
    description='Example DAG demonstrating Docker integration',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example', 'docker'],
) as dag:

    # Define a simple Python task to verify Airflow is working
    def print_hello():
        print("Hello from Airflow!")
        return "Hello from Airflow!"

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )

    # Define a Docker task to run a simple command
    docker_task = DockerOperator(
        task_id='docker_task',
        image='python:3.9-slim',
        command='python -c "import platform; print(f\'Hello from Docker! Python version: {platform.python_version()}\')"',
        docker_url='unix://var/run/docker.sock',  # Connect to Docker socket
        network_mode='bridge',
        auto_remove=True,
        retrieve_output=True,
        retrieve_output_path='/tmp/docker_output.txt',
        api_version='auto',
    )

    # Define a Docker task to run a container with volume mounts
    # This example mounts the current project directory to /app in the container
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    
    docker_volume_task = DockerOperator(
        task_id='docker_volume_task',
        image='python:3.9-slim',
        command='ls -la /app && echo "Successfully accessed mounted volume!"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        mounts=[
            Mount(source=project_dir, target='/app', type='bind')
        ],
        retrieve_output=True,
        retrieve_output_path='/tmp/docker_volume_output.txt',
        api_version='auto',
    )

    # Define a Docker task to run a container that processes news data
    docker_news_task = DockerOperator(
        task_id='docker_news_task',
        image='python:3.9-slim',
        command='cd /app && pip install -r requirements.txt && python process_news_data.py --data-dir data/news_data --index-name news_data',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        mounts=[
            Mount(source=project_dir, target='/app', type='bind')
        ],
        retrieve_output=True,
        retrieve_output_path='/tmp/docker_news_output.txt',
        api_version='auto',
    )

    # Set task dependencies
    hello_task >> docker_task >> docker_volume_task >> docker_news_task
