import logging
import sys


# def getLogger(module_name="subscript"):
def get_uploader_logger() -> logging.Logger:
    # pylint: disable=invalid-name
    """Provides a unified logger for fmu-sumo-uploader.

    Code is copied from
    https://github.com/equinor/subscript/blob/main/src/subscript/__init__.py

    Logging output is split by logging levels (split between WARNING and ERROR)
    to stdout and stderr, each log occurs in only one of the streams.

    Returns:
        A logger object
    """
    logger = logging.getLogger("fmu.sumo.uploader")
    logger.propagate = False  # Avoids duplicate logging

    if not len(logger.handlers):
        formatter = logging.Formatter(
            "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
        )

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.addFilter(lambda record: record.levelno < logging.ERROR)
        stdout_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)

        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.addFilter(
            lambda record: record.levelno >= logging.ERROR
        )
        stderr_handler.setFormatter(formatter)
        logger.addHandler(stderr_handler)

    return logger
