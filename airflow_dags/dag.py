from airflow import DAG  # noqa
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator  # noqa
import asyncio
import logging
from sls_lambda import NFTokenPriceDump, NFTokenDump, IssuerPriceDump, NFTaxonDump
from utilities import factory


logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

def xls20_data_pipeline():
    token_dump_runner = NFTokenDump(factory)
    taxon_dump_runner = NFTaxonDump(factory)
    taxon_price_runner = NFTokenPriceDump(factory)
    issuer_price_runner = IssuerPriceDump(factory)

    asyncio.run(token_dump_runner.run())
    asyncio.run(taxon_dump_runner.run())

    taxon_price_runner.run()
    issuer_price_runner.run()

    logger.info("Task Completed")


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

run_xls20_pipeline = PythonOperator(
    task_id='xls20-data-pipeline',
    python_callable=xls20_data_pipeline,
    dag=dag
)
