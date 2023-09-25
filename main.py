import asyncio
import logging
import argparse

from utilities import Factory, Config


if __name__ == "__main__":
    # Initialize the Argument Parser
    parser = argparse.ArgumentParser(
        prog="peerkat",
        description='''
        Management Command for Peerkat's XLS20 Pipelines and Scripts.
        Samples:
            python main.py --command_type=scripts --script=amount_update --env=env/.env.local
            python main.py --command_type=pipeline --stage=token-dump --env=env/.env.local
        '''
    )
    parser.add_argument("--command_type", required=True, help="Command type to run. Accepts `scripts` or `pipeline`.")
    parser.add_argument("--env", required=True, help="Path to the env file to use for the command.")
    parser.add_argument("--script", help="Specific script to run.")
    parser.add_argument("--trigger", help="Specific Trigger to operate on")
    parser.add_argument("--trigger_action", help="Action to perform on the trigger")
    parser.add_argument("--stage", help="Pipeline stage to run.")
    parser.add_argument("--endpoint", help="API endpoint to call.")
    parser.add_argument("--token_id", help="nft token id.")
    parser.add_argument("--address", help="xrpl address.")
    parser.add_argument("--page", help="for pagination")
    args = parser.parse_args()

    # Initialize Logger
    logger = logging.getLogger("app_log")
    formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    )  # noqa
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)

    # Extract the CLI Arguments
    command_type = args.command_type
    endpoint = args.endpoint
    script = args.script
    stage = args.stage
    env_path = args.env

    # Create Config and Factory for the command
    config = Config.from_env(env_path)
    factory = Factory(config)

    # Execute the command
    if command_type == 'scripts':
        from scripts import XRPAmountUpdate, CreateOfferDump, NixerOfferLoader

        if script == 'amount-update':
            runner = XRPAmountUpdate(factory)
            runner.run()
        elif script == 'create-offer-dump':
            runner = CreateOfferDump(logger)
            runner.run()
        elif script == 'nixer-offer-loader':
            runner = NixerOfferLoader(factory, logger)
            runner.run()
        else:
            logger.error("Invalid Script")
    elif command_type == 'pipeline':
        from sls_lambda import (
            CSVDump, GraphDumps, IssuerPriceDump, NFTaxonDump,
            NFTokenDump, TaxonPriceGraph, TableDump, OfferCurrencyPriceUpdate,
            TwitterDump, TaxonPriceDump, NFTSalesDump, NFTSalesGraph

        )

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
        elif stage == "offer-currency":
            runner = OfferCurrencyPriceUpdate(factory)
            runner.run()
        else:
            logger.error("Invalid Pipeline stage.")
    elif command_type == 'trigger':
        from scripts import TriggerRunner

        trigger = args.trigger
        action = args.trigger_action

        # Trigger runner Arguments
        kwargs = {
            "create_script": "",
            "function_script": "",
            "table_script": "",
            "create_table": True
        }

        # Populate the necessary arguments
        if trigger == "price-summary":
            kwargs['create_script'] = "sql/triggers/price-summary-update.sql"
            kwargs['function_script'] = "sql/functions/update-token-price-summary.sql"
            kwargs['table_script'] = "sql/tables/nft-price-summary.sql"
        elif trigger == "volume-summary":
            kwargs['create_script'] = "sql/triggers/update-nft-volume-summary.sql"
            kwargs['function_script'] = "sql/functions/update-nft-volume-summary.sql"
            kwargs['table_script'] = "sql/tables/nft-volume-summary.sql"
        elif trigger == "nft-burn-offer":
            kwargs['create_script'] = "sql/triggers/update-burn-offer-hash.sql"
            kwargs['function_script'] = "sql/functions/update-burn-offer-hash.sql"
            kwargs['create_table'] = False
        elif trigger == "nft-sales":
            kwargs['create_script'] = "sql/triggers/update-nft-sales.sql"
            kwargs['function_script'] = "sql/functions/update-nft-sales.sql"
            kwargs['table_script'] = "sql/tables/nft-sales.sql"
        elif trigger == "nft-offer-currency":
            kwargs['create_script'] = "sql/triggers/create_offer_currency.sql"
            kwargs['function_script'] = "sql/functions/add_new_offer_currency.sql"
        else:
            logger.error("Invalid Trigger Type")
            exit(1)

        # Run the Trigger
        runner = TriggerRunner(factory, logger)
        runner.run(action=action, **kwargs)
    elif command_type == 'api':
        from sls_lambda import TokenHistoryFetcher, AccountActivity, AccountNFTS

        token_id = args.token_id
        address = args.address
        page = args.page if args.page else 0
        if endpoint == 'token-history':
            if token_id is None:
                logger.error('Token id required')
                exit(1)
            fetcher = TokenHistoryFetcher(factory)
            response = fetcher.fetch_history(token_id)
            # for data in response:
            #     print(data['action'])
        elif endpoint == "account-activity":
            if address is None:
                logger.error('Address required')
                exit(1)
            # Page starts from zero
            fetcher = AccountActivity(factory)
            response = fetcher.fetch_activity(address, page)
            print(response)
        elif endpoint == "account-nfts":
            """
            python main.py --command_type=api --endpoint=account-nfts --address=rJKnmyqZJgnvHiWv2SPxaNcxhp9LLW8syT --page=1 --env=env/.env.local
            """
            if address is None:
                logger.error('Address required')
                exit(1)
            if int(page) <= 0:
                logger.error('invalid page number')
                exit(1)
            offset = (int(page) - 1) * 10
            fetcher = AccountNFTS(factory)
            response = fetcher.fetch(address, offset)
            print(response)
        else:
            logger.error('Invalid endpoint')
    else:
        logger.error("Invalid Command Type")
