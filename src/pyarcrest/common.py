import logging


# taken from http.client default blocksize
HTTP_BUFFER_SIZE = 8192


def getNullLogger():
    logger = logging.getLogger('null')
    if not logger.hasHandlers():
        logger.addHandler(logging.NullHandler())
    return logger
