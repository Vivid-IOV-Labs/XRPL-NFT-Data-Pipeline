from airflow import DAG  # noqa
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator  # noqa
import asyncio
import logging
# from sls_lambda import NFTokenPriceDump, NFTokenDump, IssuerPriceDump, NFTaxonDump
# from utilities import factory


logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def token_dump():
    from utilities import factory
    from sls_lambda import NFTokenDump

    token_dump_runner = NFTokenDump(factory)
    asyncio.run(token_dump_runner.run())

def taxon_dump():
    from utilities import factory
    from sls_lambda import NFTaxonDump

    taxon_dump_runner = NFTaxonDump(factory)
    asyncio.run(taxon_dump_runner.run())

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


# def xls20_data_pipeline():
#     token_dump_runner = NFTokenDump(factory)
#     taxon_dump_runner = NFTaxonDump(factory)
#     taxon_price_runner = NFTokenPriceDump(factory)
#     issuer_price_runner = IssuerPriceDump(factory)
#
#     asyncio.run(token_dump_runner.run())
#     asyncio.run(taxon_dump_runner.run())
#
#     taxon_price_runner.run()
#     issuer_price_runner.run()
#
#     logger.info("Task Completed")


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
    schedule="@hourly",
    description="This DAG is for XLS20 Data pipeline."
)

run_token_dump = PythonOperator(
    task_id='token-dump',
    python_callable=token_dump,
    dag=dag
)

run_taxon_dump = PythonOperator(
    task_id='taxon-dump',
    python_callable=taxon_dump,
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

# run_xls20_pipeline = PythonOperator(
#     task_id='xls20-data-pipeline',
#     python_callable=xls20_data_pipeline,
#     dag=dag
# )
# print("yes")

run_token_dump >> run_taxon_dump >> run_taxon_pricing >> run_issuer_pricing
