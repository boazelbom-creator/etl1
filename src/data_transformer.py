from src.logger import get_logger

logger = get_logger(__name__)


class DataTransformer:
    """
    Transforms parsed data into batches for database insertion.
    """

    def __init__(self, batch_size=1000):
        """
        Initialize DataTransformer with batch size.

        Args:
            batch_size (int): Number of records per batch (default: 1000)
        """
        self.batch_size = batch_size
        logger.info(f"DataTransformer initialized with batch_size={batch_size}")

    def create_batches(self, data):
        """
        Split data into batches of specified size.

        Args:
            data (list): List of records (posts or comments)

        Returns:
            list: List of batches, where each batch is a list of records

        Example:
            Input: [1,2,3,4,5], batch_size=2
            Output: [[1,2], [3,4], [5]]
        """
        if not data:
            logger.warning("No data provided for batching")
            return []

        batches = []
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            batches.append(batch)

        logger.info(f"Created {len(batches)} batches from {len(data)} records")
        return batches

    def validate_post(self, post):
        """
        Validate that a post record has all required fields.

        Args:
            post (dict): Post record

        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = ["post_id", "timestamp", "title", "post_texts", "text_length"]

        for field in required_fields:
            if field not in post:
                logger.warning(f"Post missing required field: {field}")
                return False

        if not post["post_id"]:
            logger.warning("Post has empty post_id")
            return False

        return True

    def validate_comment(self, comment):
        """
        Validate that a comment record has all required fields.

        Args:
            comment (dict): Comment record

        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = ["comment_id", "post_id", "timestamp", "author", "comment_texts", "text_length"]

        for field in required_fields:
            if field not in comment:
                logger.warning(f"Comment missing required field: {field}")
                return False

        if not comment["comment_id"]:
            logger.warning("Comment has empty comment_id")
            return False

        if not comment["post_id"]:
            logger.warning("Comment has empty post_id")
            return False

        return True

    def filter_valid_records(self, records, record_type="post"):
        """
        Filter out invalid records from the list.

        Args:
            records (list): List of records to validate
            record_type (str): Type of record ("post" or "comment")

        Returns:
            list: List of valid records
        """
        validate_func = self.validate_post if record_type == "post" else self.validate_comment

        valid_records = []
        invalid_count = 0

        for record in records:
            if validate_func(record):
                valid_records.append(record)
            else:
                invalid_count += 1

        if invalid_count > 0:
            logger.warning(f"Filtered out {invalid_count} invalid {record_type} records")

        logger.info(f"Validated {len(valid_records)} {record_type} records")
        return valid_records
