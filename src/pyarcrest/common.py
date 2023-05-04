import logging


def getNullLogger():
    logger = logging.getLogger('null')
    if not logger.hasHandlers():
        logger.addHandler(logging.NullHandler())
    return logger
