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


def offer_currency_price():
    from sls_lambda import OfferCurrencyPriceUpdate
    from utilities import Factory, Config

    config = Config.from_env(".env")
    factory = Factory(config)
    runner = OfferCurrencyPriceUpdate(factory)
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
    dag_id="NFT_OFFER_CURRENCY_PRICE_UPDATE",
    default_args=default_args,
    schedule_interval="0 */12 * * *",
    description="Updates Price of offer currencies.",
    start_date=datetime(2023, 8, 8),
    catchup=False,
) as dag:
    run_offer_currency_price_update = PythonOperator(
        task_id="offer-currency-price-update",
        python_callable=offer_currency_price
    )

    run_offer_currency_price_update  # noqa
