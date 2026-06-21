import logging
import sys

_RESET = "\033[0m"

_LEVEL_COLORS = {
    logging.DEBUG:    "\033[90m",    # grey
    logging.INFO:     "\033[94m",    # blue
    logging.WARNING:  "\033[93m",    # yellow
    logging.ERROR:    "\033[91m",    # red
    logging.CRITICAL: "\033[1;91m",  # bold red
}

_DEFAULT_FMT = "%(asctime)s [%(levelname)s] %(message)s"


class ColorFormatter(logging.Formatter):
    """Logging formatter that applies ANSI colour codes based on log level."""

    def format(self, record: logging.LogRecord) -> str:
        color = _LEVEL_COLORS.get(record.levelno, "")
        return f"{color}{super().format(record)}{_RESET}"


def get_color_logger(name: str, fmt: str = _DEFAULT_FMT) -> logging.Logger:
    """Return a logger that emits colour-coded output to stderr.

    Sets propagate=False so messages are not duplicated on the root logger.
    """
    logger = logging.getLogger(name)
    logger.propagate = False
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(ColorFormatter(fmt))
    logger.addHandler(handler)
    return logger
