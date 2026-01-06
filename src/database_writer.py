import psycopg2
from psycopg2.extras import execute_batch
from src.logger import get_logger

logger = get_logger(__name__)


class DatabaseWriter:
    """
    Handles PostgreSQL database connections and batch UPSERT operations.
    """

    def __init__(self, db_config):
        """
        Initialize DatabaseWriter with database configuration.

        Args:
            db_config (dict): Database configuration containing host, database, username, password, port
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        logger.info("DatabaseWriter initialized")

    def connect(self):
        """
        Establish connection to PostgreSQL database.

        Raises:
            psycopg2.Error: If connection fails
        """
        try:
            self.connection = psycopg2.connect(
                host=self.db_config.get("host"),
                database=self.db_config.get("database"),
                user=self.db_config.get("username"),
                password=self.db_config.get("password"),
                port=self.db_config.get("port", 5432)
            )
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to database: {self.db_config.get('database')}")

        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")

    def insert_posts_batch(self, posts_batch):
        """
        Insert a batch of posts using UPSERT logic (INSERT ... ON CONFLICT DO UPDATE).

        Args:
            posts_batch (list): List of post dictionaries

        Returns:
            tuple: (success_count, failure_count)
        """
        if not posts_batch:
            logger.warning("Empty posts batch provided")
            return 0, 0

        upsert_query = """
            INSERT INTO posts (post_id, timestamp, title, post_texts, text_length)
            VALUES (%(post_id)s, %(timestamp)s, %(title)s, %(post_texts)s, %(text_length)s)
            ON CONFLICT (post_id)
            DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                title = EXCLUDED.title,
                post_texts = EXCLUDED.post_texts,
                text_length = EXCLUDED.text_length;
        """

        try:
            execute_batch(self.cursor, upsert_query, posts_batch, page_size=len(posts_batch))
            self.connection.commit()
            logger.info(f"Successfully inserted/updated {len(posts_batch)} posts")
            return len(posts_batch), 0

        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to insert posts batch: {e}")
            return 0, len(posts_batch)

    def insert_comments_batch(self, comments_batch):
        """
        Insert a batch of comments using UPSERT logic (INSERT ... ON CONFLICT DO UPDATE).

        Args:
            comments_batch (list): List of comment dictionaries

        Returns:
            tuple: (success_count, failure_count)
        """
        if not comments_batch:
            logger.warning("Empty comments batch provided")
            return 0, 0

        upsert_query = """
            INSERT INTO comments (comment_id, post_id, timestamp, author, comment_texts, text_length)
            VALUES (%(comment_id)s, %(post_id)s, %(timestamp)s, %(author)s, %(comment_texts)s, %(text_length)s)
            ON CONFLICT (comment_id)
            DO UPDATE SET
                post_id = EXCLUDED.post_id,
                timestamp = EXCLUDED.timestamp,
                author = EXCLUDED.author,
                comment_texts = EXCLUDED.comment_texts,
                text_length = EXCLUDED.text_length;
        """

        try:
            execute_batch(self.cursor, upsert_query, comments_batch, page_size=len(comments_batch))
            self.connection.commit()
            logger.info(f"Successfully inserted/updated {len(comments_batch)} comments")
            return len(comments_batch), 0

        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to insert comments batch: {e}")
            return 0, len(comments_batch)

    def process_batches(self, batches, batch_type="posts"):
        """
        Process multiple batches and track success/failure statistics.

        Args:
            batches (list): List of batches to process
            batch_type (str): Type of data ("posts" or "comments")

        Returns:
            dict: Statistics about the processing (total, success, failed)
        """
        total_records = sum(len(batch) for batch in batches)
        total_success = 0
        total_failed = 0

        insert_func = self.insert_posts_batch if batch_type == "posts" else self.insert_comments_batch

        logger.info(f"Processing {len(batches)} batches of {batch_type} ({total_records} total records)")

        for i, batch in enumerate(batches, 1):
            logger.info(f"Processing {batch_type} batch {i}/{len(batches)} ({len(batch)} records)")

            success, failed = insert_func(batch)
            total_success += success
            total_failed += failed

            if failed > 0:
                logger.warning(f"Batch {i} had {failed} failures, continuing with next batch")

        stats = {
            "total_records": total_records,
            "total_batches": len(batches),
            "success": total_success,
            "failed": total_failed
        }

        logger.info(f"{batch_type.capitalize()} processing complete: {stats}")
        return stats

    def verify_tables_exist(self):
        """
        Verify that required tables (posts and comments) exist in the database.

        Returns:
            bool: True if tables exist, False otherwise
        """
        try:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'posts'
                );
            """)
            posts_exists = self.cursor.fetchone()[0]

            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'comments'
                );
            """)
            comments_exists = self.cursor.fetchone()[0]

            if posts_exists and comments_exists:
                logger.info("Verified: posts and comments tables exist")
                return True
            else:
                logger.error("Required tables do not exist in database")
                return False

        except psycopg2.Error as e:
            logger.error(f"Failed to verify tables: {e}")
            return False
