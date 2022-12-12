import logging
from handlers import issuers_nft_dumps, issuers_taxon_dumps


logger = logging.getLogger("app_log")
file_handler = logging.FileHandler("logger.log")
logger.addHandler(file_handler)


