from datetime import datetime, timedelta

from airflow import DAG  # noqa
from airflow.operators.bash import BashOperator  # noqa


default_args = {
    "owner": "peerkat",
    "depends_on_past": False,
    "email": ["ike@peerkat.com", "emmanueloluwatobi2000@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="LOG_CLEANUP_DAG",
    default_args=default_args,
    schedule_interval="@hourly",
    description="Cleanup Logs.",
    start_date=datetime(2023, 3, 28),
    catchup=False,
) as dag:
    AIRFLOW_LOG_PATH = "/Users/teepy/workspace/Peerkat/Python-Serverless-V2/logs"
    clean_scheduler_logs = BashOperator(
        task_id='clean_scheduler_logs',
        bash_command=f"find $AIRFLOW_HOME/logs -type f -delete"
    )
    clean_scheduler_logs
