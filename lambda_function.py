"""
AWS Lambda function for Facebook JSON to PostgreSQL ETL.

This Lambda function reads Facebook post and comment data from JSON files in S3,
transforms the data, and loads it into PostgreSQL tables using batch processing
with UPSERT logic for duplicate handling.
"""

from src.config_manager import ConfigManager
from src.s3_reader import S3Reader
from src.json_parser import FacebookJSONParser
from src.data_transformer import DataTransformer
from src.database_writer import DatabaseWriter
from src.logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event, context):
    """
    Main Lambda handler function.

    Args:
        event (dict): Lambda event object
        context (object): Lambda context object

    Returns:
        dict: Response with status code and processing summary
    """
    logger.info("=" * 80)
    logger.info("Lambda function execution started")
    logger.info("=" * 80)

    try:
        # 1. Load Configuration
        logger.info("Step 1: Loading configuration...")
        config_manager = ConfigManager()
        config_manager.validate()

        s3_config = config_manager.get_s3_config()
        db_config = config_manager.get_database_config()
        batch_size = config_manager.get_batch_size()

        # 2. Read JSON from S3
        logger.info("Step 2: Reading JSON from S3...")
        s3_reader = S3Reader(s3_config)

        if not s3_reader.file_exists():
            error_msg = f"File not found in S3: {s3_reader.get_s3_key()}"
            logger.error(error_msg)
            return {
                'statusCode': 404,
                'body': {'error': error_msg}
            }

        json_content = s3_reader.read_json_file()

        # 3. Parse JSON
        logger.info("Step 3: Parsing JSON content...")
        parser = FacebookJSONParser()
        posts, comments = parser.parse(json_content)

        if not posts and not comments:
            logger.warning("No posts or comments found in JSON")
            return {
                'statusCode': 200,
                'body': {
                    'message': 'No data to process',
                    'posts_processed': 0,
                    'comments_processed': 0
                }
            }

        # 4. Transform and Validate Data
        logger.info("Step 4: Transforming and validating data...")
        transformer = DataTransformer(batch_size=batch_size)

        valid_posts = transformer.filter_valid_records(posts, record_type="post")
        valid_comments = transformer.filter_valid_records(comments, record_type="comment")

        posts_batches = transformer.create_batches(valid_posts)
        comments_batches = transformer.create_batches(valid_comments)

        # 5. Connect to Database
        logger.info("Step 5: Connecting to database...")
        db_writer = DatabaseWriter(db_config)
        db_writer.connect()

        # Verify tables exist
        if not db_writer.verify_tables_exist():
            error_msg = "Required database tables do not exist"
            logger.error(error_msg)
            db_writer.disconnect()
            return {
                'statusCode': 500,
                'body': {'error': error_msg}
            }

        # 6. Process Posts Batches
        logger.info("Step 6: Processing posts batches...")
        posts_stats = db_writer.process_batches(posts_batches, batch_type="posts")

        # 7. Process Comments Batches
        logger.info("Step 7: Processing comments batches...")
        comments_stats = db_writer.process_batches(comments_batches, batch_type="comments")

        # 8. Disconnect from Database
        logger.info("Step 8: Closing database connection...")
        db_writer.disconnect()

        # 9. Prepare Response
        response = {
            'statusCode': 200,
            'body': {
                'message': 'ETL process completed successfully',
                'posts': {
                    'total': posts_stats['total_records'],
                    'batches': posts_stats['total_batches'],
                    'success': posts_stats['success'],
                    'failed': posts_stats['failed']
                },
                'comments': {
                    'total': comments_stats['total_records'],
                    'batches': comments_stats['total_batches'],
                    'success': comments_stats['success'],
                    'failed': comments_stats['failed']
                }
            }
        }

        logger.info("=" * 80)
        logger.info("Lambda function execution completed successfully")
        logger.info(f"Posts: {posts_stats['success']}/{posts_stats['total_records']} successful")
        logger.info(f"Comments: {comments_stats['success']}/{comments_stats['total_records']} successful")
        logger.info("=" * 80)

        return response

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"Lambda function execution failed: {str(e)}")
        logger.error("=" * 80)

        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'ETL process failed'
            }
        }


# For local testing
if __name__ == "__main__":
    # Mock event and context for local testing
    test_event = {}
    test_context = {}

    result = lambda_handler(test_event, test_context)
    print("\nLambda Response:")
    print(result)
