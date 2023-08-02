import asyncio
import logging
import sys
import argparse
import time

from sls_lambda import (CSVDump, GraphDumps, IssuerPriceDump, NFTaxonDump,
                        NFTokenDump, TaxonPriceGraph, TableDump, TwitterDump, TaxonPriceDump, NFTSalesDump, NFTSalesGraph)
from utilities import Factory, Config
from scripts import XRPAmountUpdate, CreateOfferDump

logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    # Initialize the Argument Parser
    parser = argparse.ArgumentParser(
        prog="peerkat",
        description='''
        Management Command for Peerkat's XLS20 Pipelines and Scripts.
        Samples:
            python main.py --command_type=scripts --script=amount_update
            python main.py --command_type=pipeline --stage=token-dump
        '''
    )
    parser.add_argument("--command_type", required=True, help="Command type to run. Accepts `scripts` or `pipeline`.")
    parser.add_argument("--env", required=True, help="Path to the env file to use for the command.")
    parser.add_argument("--script", help="Specific script to run.")
    parser.add_argument("--stage", help="Pipeline stage to run.")
    args = parser.parse_args()

    # Extract the CLI Arguments
    command_type = args.command_type
    script = args.script
    stage = args.stage
    env_path = args.env

    # Create Config and Factory for the command
    config = Config.from_env(env_path)
    factory = Factory(config)

    # Execute the command
    if command_type == 'scripts':
        if script == 'amount-update':
            runner = XRPAmountUpdate(factory)
            runner.run()
        elif script == 'create-offer-dump':
            runner = CreateOfferDump(logger)
            runner.run()
        else:
            logger.error("Invalid Script")
    elif command_type == 'pipeline':
        if stage == 'token-dump':
            runner = NFTokenDump(factory)
            runner.run()
        elif stage == 'taxon-dump':
            runner = NFTaxonDump(factory)
            runner.run()
        elif stage == "issuer-pricing":
            runner = IssuerPriceDump(factory)
            asyncio.run(runner.run())
        elif stage == "taxon-pricing":
            runner = TaxonPriceDump(factory)
            runner.run()
        elif stage == "csv-dump":
            runner = CSVDump(factory)
            runner.run()
        elif stage == "table-dump":
            runner = TableDump(factory)
            runner.sync_run()
        elif stage == "graph-dump":
            runner = GraphDumps(factory)
            runner.run()
        elif stage == "taxon-price-graph":
            runner = TaxonPriceGraph(factory)
            asyncio.run(runner.run())
        elif stage == "twitter-dump":
            runner = TwitterDump(factory)
            runner.run()
        elif stage == "sales-dump":
            runner = NFTSalesDump(factory)
            runner.run()
        elif stage == "sales-graph":
            runner = NFTSalesGraph(factory)
            runner.run()
        else:
            logger.error("Invalid Pipeline stage.")
    else:
        logger.error("Invalid Command Type")
    # section = sys.argv[1]
    # start = time.time()



    # else:
    #     logger.info(
    #         "Invalid Option. Available options are `token-dump, taxon-dump, taxon-pricing, issuer-pricing, table-dump`"
    #     )
    # logger.info(f"Executed in {time.time() - start}")
