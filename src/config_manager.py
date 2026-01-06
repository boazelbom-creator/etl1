import json
import os
from src.logger import get_logger

logger = get_logger(__name__)


class ConfigManager:
    """
    Manages configuration loading and validation for the Lambda function.
    Supports loading from config file or environment variables.
    """

    def __init__(self, config_path=None):
        """
        Initialize ConfigManager with optional config file path.

        Args:
            config_path (str): Path to config.json file. If None, uses default path.
        """
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'config',
                'config.json'
            )

        self.config = self._load_config(config_path)
        logger.info("Configuration loaded successfully")

    def _load_config(self, config_path):
        """
        Load configuration from file or environment variables.

        Args:
            config_path (str): Path to config file

        Returns:
            dict: Configuration dictionary

        Raises:
            FileNotFoundError: If config file not found
            json.JSONDecodeError: If config file is malformed
        """
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {config_path}")
            return config
        except FileNotFoundError:
            logger.warning(f"Config file not found at {config_path}, loading from environment variables")
            return self._load_from_env()
        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON in config file: {e}")
            raise

    def _load_from_env(self):
        """
        Load configuration from environment variables (fallback).

        Returns:
            dict: Configuration dictionary from environment variables
        """
        return {
            "s3": {
                "bucket_name": os.getenv("S3_BUCKET_NAME"),
                "folder_path": os.getenv("S3_FOLDER_PATH", ""),
                "file_name": os.getenv("S3_FILE_NAME")
            },
            "database": {
                "host": os.getenv("DB_HOST"),
                "database": os.getenv("DB_NAME"),
                "username": os.getenv("DB_USERNAME"),
                "password": os.getenv("DB_PASSWORD"),
                "port": int(os.getenv("DB_PORT", 5432))
            },
            "batch_size": int(os.getenv("BATCH_SIZE", 1000))
        }

    def get_s3_config(self):
        """Get S3 configuration."""
        return self.config.get("s3", {})

    def get_database_config(self):
        """Get database configuration."""
        return self.config.get("database", {})

    def get_batch_size(self):
        """Get batch size for processing."""
        return self.config.get("batch_size", 1000)

    def validate(self):
        """
        Validate that all required configuration parameters are present.

        Raises:
            ValueError: If required configuration is missing
        """
        s3_config = self.get_s3_config()
        db_config = self.get_database_config()

        required_s3 = ["bucket_name", "file_name"]
        required_db = ["host", "database", "username", "password", "port"]

        for key in required_s3:
            if not s3_config.get(key):
                raise ValueError(f"Missing required S3 configuration: {key}")

        for key in required_db:
            if not db_config.get(key):
                raise ValueError(f"Missing required database configuration: {key}")

        logger.info("Configuration validation passed")
