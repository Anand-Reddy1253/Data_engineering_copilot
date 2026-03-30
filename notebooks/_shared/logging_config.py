"""Structured logging configuration for the Contoso Fabric Platform.

Configures Python logging with JSON-formatted output for consistency
across all notebooks and shared modules.
"""

import json
import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Custom JSON log formatter for structured logging.

    Formats log records as JSON objects with timestamp, level, logger name,
    message, and optional extra fields.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string.

        Args:
            record: The log record to format.

        Returns:
            JSON-formatted log string.
        """
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_entry["exception"] = traceback.format_exception(*record.exc_info)

        # Add any extra fields passed via logger.info(..., extra={...})
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
            ):
                if not key.startswith("_"):
                    log_entry[key] = value

        return json.dumps(log_entry)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Create or retrieve a named logger with JSON formatting.

    Args:
        name: Logger name (use __name__ for module-level loggers).
        level: Log level string. Defaults to INFO. Can be overridden
               with the LOG_LEVEL environment variable.

    Returns:
        Configured logger instance.

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing %d rows", row_count)
        >>> logger.error("Failed to read Delta table", extra={"path": delta_path})
    """
    effective_level = os.environ.get("LOG_LEVEL", level).upper()

    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    logger.setLevel(getattr(logging, effective_level, logging.INFO))
    logger.propagate = False

    return logger
