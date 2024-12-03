import logging
import os


def get_db_url() -> str:
    """Construct database URL from environment variables."""
    return (
        f"postgresql+asyncpg://{os.getenv('POSTGRES_USER', 'postgres')}:{os.getenv('POSTGRES_PASSWORD', 'postgres')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.getenv('POSTGRES_DB', 'postgres')}"
    )


def get_mqtt_config() -> tuple[str, int]:
    """Get MQTT configuration from environment variables."""
    return (os.getenv("MQTT_HOST", "localhost"), int(os.getenv("MQTT_PORT", "1883")))


def setup_logging(name: str = "MQTTDataPipeline") -> logging.Logger:
    """Configure logging with level from environment variable."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    return logging.Logger(name=name)


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
