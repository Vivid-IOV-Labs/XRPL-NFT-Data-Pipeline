from airflow import DAG  # noqa
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator  # noqa
import asyncio
import logging


logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


async def token_taxon_invoker():
    import sys
    import os

    working_dir = os.getcwd()
    sys.path.append(f"{working_dir}/sls_lambda")
    from sls_lambda.invokers import invoke_token_dumps, invoke_taxon_dumps
    from utilities import factory
    await asyncio.gather(*[invoke_token_dumps(factory.config), invoke_taxon_dumps(factory.config)])

def token_taxon_dump():
    asyncio.run(token_taxon_invoker())

def taxon_pricing():
    from utilities import factory
    from sls_lambda import NFTokenPriceDump

    taxon_price_runner = NFTokenPriceDump(factory)
    taxon_price_runner.run()

def issuer_pricing():
    from utilities import factory
    from sls_lambda import IssuerPriceDump

    issuer_price_runner = IssuerPriceDump(factory)
    issuer_price_runner.run()

def csv_dump():
    from sls_lambda.invokers import invoke_csv_dump
    from utilities import factory

    invoke_csv_dump(factory.config)


default_args = {
    "owner": "peerkat",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "email": ["ike@peerkat.com", "emmanueloluwatobi2000@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "XLS20_Data_Pipeline_DAG",
    default_args=default_args,
    schedule="@hourly",
    description="This DAG is for XLS20 Data pipeline."
)

run_token_taxon_dump = PythonOperator(
    task_id='token-taxon-dump',
    python_callable=token_taxon_dump,
    dag=dag
)

run_taxon_pricing = PythonOperator(
    task_id='taxon-pricing',
    python_callable=taxon_pricing,
    dag=dag
)

run_issuer_pricing = PythonOperator(
    task_id='issuer-pricing',
    python_callable=issuer_pricing,
    dag=dag
)

run_csv_dump = PythonOperator(
    task_id='csv-dump',
    python_callable=csv_dump,
    dag=dag
)

run_token_taxon_dump >> run_taxon_pricing >> run_issuer_pricing >> run_csv_dump
