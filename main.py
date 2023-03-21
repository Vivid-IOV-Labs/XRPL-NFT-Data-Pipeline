import logging
from sls_lambda import NFTokenPriceDump, IssuerPriceDump
from utilities import factory


logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    token_price_runner = NFTokenPriceDump(factory)
    # issuer_price_runner = IssuerPriceDump(factory)

    token_price_runner.run()
    # issuer_price_runner.run()
