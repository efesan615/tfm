# src/tfm/logging_cfg.py
import logging
from logging.config import dictConfig

def setup_logging(level: str = "INFO") -> None:
    dictConfig({
        "version": 1,
        "formatters": {"std": {"format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"}},
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "std", "level": level}
        },
        "root": {"handlers": ["console"], "level": level},
    })
