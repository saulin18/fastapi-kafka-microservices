import logging
import logging.config
import os
from typing import Any


def build_logging_config(level: str = "INFO") -> dict[str, Any]:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level,
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",
                "formatter": "detailed",
                "filename": "logs/app.log",
                "maxBytes": 10_485_760,
                "backupCount": 5,
                "encoding": "utf8",
            },
        },
        "root": {
            "handlers": ["console", "file"],
            "level": level,
        },
    }


def setup_logging(level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    logging.config.dictConfig(build_logging_config(level))
