import logging
from datetime import datetime, timedelta

from airflow import DAG  # noqa
from airflow.operators.python import PythonOperator  # noqa

logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def twitter_social_activity_dump():
    from sls_lambda import TwitterDump
    from utilities import factory

    runner = TwitterDump(factory)
    runner.run()

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
    dag_id="TWITTER_DAILY_DUMPS",
    default_args=default_args,
    schedule_interval="@daily",
    description="This DAG is for twitter social activity dump of all the tracked xls20 nft issuer accounts.",
    start_date=datetime(2023, 6, 20),
    catchup=False,
) as dag:
    run_taxon_price_summary = PythonOperator(
        task_id="twitter-social-activity",
        python_callable=twitter_social_activity_dump
    )

    run_taxon_price_summary
