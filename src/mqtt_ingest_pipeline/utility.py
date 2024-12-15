import logging
import os


class CustomLogger:
    @staticmethod
    def get_logger(name: str, level: int | None = None) -> logging.Logger:
        if level is None:
            # Get the log level from the environment variable, defaulting to INFO if not set
            env_level = os.getenv("LOG_LEVEL", "INFO").upper()

            # Convert the log level string to an actual logging level
            level = getattr(logging, env_level, logging.INFO)

        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger
