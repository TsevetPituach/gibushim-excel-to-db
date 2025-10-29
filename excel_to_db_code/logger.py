import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    """Configure root logger with a concise format.

    Parameters
    ----------
    level : str
        Logging level name (e.g., "DEBUG", "INFO", "WARNING").
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Avoid duplicate handlers if setup_logging is called multiple times
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(numeric_level)
        return

    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s: %(message)s"
    datefmt = "%H:%M:%S"
    handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))

    root.setLevel(numeric_level)
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

