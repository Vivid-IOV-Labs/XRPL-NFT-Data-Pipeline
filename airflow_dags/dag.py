from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import json
import pendulum

def test_dag():
    print("Dag is active!!!!!")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 23),
    "email": ["ike@peerkat.com", "emmanueloluwatobi2000@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


dag = DAG(
    "XLS20_Data_Pipeline_DAG",
    default_args=default_args,
    description="This DAG is for XLS20 Data pipeline."
)

run_test_dag = PythonOperator(
    task_id='test_dag',
    python_callable=test_dag,
    dag=dag
)