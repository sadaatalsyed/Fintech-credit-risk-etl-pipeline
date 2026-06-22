"""
utils/logging_setup.py
=======================
Structured logging configuration shared by every pipeline stage.

Replaces the original notebooks' print()-based progress messages with proper
logging: timestamps, severity levels, and a consistent format that's usable
both in the console and in production log aggregation tools.
"""

import logging
import sys


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Return a configured logger for a given pipeline stage.

    Parameters
    ----------
    name : str
        Logger name, typically __name__ of the calling module.
    level : int
        Logging level (default: logging.INFO).

    Returns
    -------
    logging.Logger
        A logger with a single stream handler attached (idempotent --
        calling this multiple times for the same name will not duplicate
        handlers).
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    return logger
