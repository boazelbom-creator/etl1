import boto3
from botocore.exceptions import ClientError
from src.logger import get_logger

logger = get_logger(__name__)


class S3Reader:
    """
    Handles reading JSON files from AWS S3.
    """

    def __init__(self, s3_config):
        """
        Initialize S3Reader with configuration.

        Args:
            s3_config (dict): S3 configuration containing bucket_name, folder_path, file_name
        """
        self.bucket_name = s3_config.get("bucket_name")
        self.folder_path = s3_config.get("folder_path", "")
        self.file_name = s3_config.get("file_name")
        self.s3_client = boto3.client('s3')

        logger.info(f"S3Reader initialized for bucket: {self.bucket_name}")

    def get_s3_key(self):
        """
        Construct the full S3 key from folder path and file name.

        Returns:
            str: Full S3 object key
        """
        if self.folder_path:
            return f"{self.folder_path.rstrip('/')}/{self.file_name}"
        return self.file_name

    def read_json_file(self):
        """
        Read JSON file from S3 and return its content as a string.

        Returns:
            str: JSON file content as string

        Raises:
            ClientError: If S3 operation fails
            Exception: For other unexpected errors
        """
        s3_key = self.get_s3_key()

        try:
            logger.info(f"Reading file from S3: s3://{self.bucket_name}/{s3_key}")

            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )

            json_content = response['Body'].read().decode('utf-8')

            file_size_mb = len(json_content) / (1024 * 1024)
            logger.info(f"Successfully read {file_size_mb:.2f} MB from S3")

            return json_content

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"File not found in S3: s3://{self.bucket_name}/{s3_key}")
            elif error_code == 'NoSuchBucket':
                logger.error(f"Bucket not found: {self.bucket_name}")
            else:
                logger.error(f"S3 ClientError: {error_code} - {e.response['Error']['Message']}")
            raise

        except Exception as e:
            logger.error(f"Unexpected error reading from S3: {str(e)}")
            raise

    def file_exists(self):
        """
        Check if the file exists in S3.

        Returns:
            bool: True if file exists, False otherwise
        """
        s3_key = self.get_s3_key()

        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            logger.info(f"File exists: s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError:
            logger.warning(f"File does not exist: s3://{self.bucket_name}/{s3_key}")
            return False
