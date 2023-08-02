import asyncio
import sys
from logging import Logger

from utilities import Factory, TriggerManager


class TriggerRunner:
    def __init__(self, factory: Factory, logger: Logger):
        self.factory = factory
        self.logger = logger

    async def _run(self, action: str, **kwargs):
        create_script = kwargs.get("create_script")
        function_script = kwargs.get("function_script")
        table_script = kwargs.get("table_script")
        create_table = kwargs.get("create_table", True)

        manager = TriggerManager(
            factory=self.factory,
            trigger_create_script=create_script,
            function_script=function_script,
            table_create_script=table_script,
            allow_table_create=create_table
        )

        if action == "create":
            await manager.create_trigger()
        elif action == "update-function":
            await manager.update_function()
        else:
            self.logger.error("Invalid Action. Supported Actions are `create`, `update-function`")

    def run(self, action: str, **kwargs):
        asyncio.run(self._run(action, **kwargs))


if __name__ == "__main__":
    args = sys.argv
    if args[1] == "price-summary":
        asyncio.run(nft_pricing_summary("create"))
    elif args[1] == "volume-summary":
        asyncio.run(nft_volume_summary("create"))
    elif args[1] == "nft-burn-offer":
        asyncio.run(nft_burn_offer("create"))
    elif args[1] == "nft-sales":
        asyncio.run(nft_sales("create"))