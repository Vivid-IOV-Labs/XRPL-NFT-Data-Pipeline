import asyncio
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


def taxon_price_summary():
    from sls_lambda import TaxonPriceDump
    from utilities import factory

    runner = TaxonPriceDump(factory)
    runner.run()

def project_price_graphs():
    from sls_lambda import TaxonPriceGraph
    from utilities import factory

    runner = TaxonPriceGraph(factory)
    asyncio.run(runner.run())

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
    dag_id="NFT_PROJECT_PRICE_GRAPHS",
    default_args=default_args,
    schedule_interval="@hourly",
    description="This DAG is for xls20 nft issuer project price graphs.",
    start_date=datetime(2023, 3, 28),
    catchup=False,
) as dag:
    run_taxon_price_summary = PythonOperator(
        task_id="taxon-price-summary",
        python_callable=taxon_price_summary
    )

    run_project_price_graphs = PythonOperator(
        task_id="project-price-graphs",
        python_callable=project_price_graphs,
    )

    run_taxon_price_summary >> run_project_price_graphs
