import json
import logging
import os
import traceback
from datetime import datetime


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            log_entry["exception"] = traceback.format_exception(*record.exc_info)
        return json.dumps(log_entry, default=str)


def configure_logging():
    handler = logging.StreamHandler()
    if os.environ.get("LOG_FORMAT", "json") == "json":
        handler.setFormatter(JSONFormatter())
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, handlers=[handler], force=True)
