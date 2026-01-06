import json
from datetime import datetime
from src.logger import get_logger

logger = get_logger(__name__)


class FacebookJSONParser:
    """
    Parses Facebook JSON export format and extracts posts and comments.
    """

    def __init__(self):
        """Initialize the parser."""
        self.posts = []
        self.comments = []
        logger.info("FacebookJSONParser initialized")

    def parse(self, json_content):
        """
        Parse Facebook JSON content and extract posts and comments.

        Args:
            json_content (str): JSON content as string

        Returns:
            tuple: (posts_list, comments_list)

        Raises:
            json.JSONDecodeError: If JSON is malformed
            KeyError: If expected keys are missing
        """
        try:
            data = json.loads(json_content)
            logger.info("JSON content parsed successfully")

            self.posts = self._extract_posts(data.get("posts", []))
            self.comments = self._extract_comments(data.get("comments", []))

            logger.info(f"Extracted {len(self.posts)} posts and {len(self.comments)} comments")

            return self.posts, self.comments

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during parsing: {e}")
            raise

    def _extract_posts(self, posts_data):
        """
        Extract and transform posts from JSON data.

        Args:
            posts_data (list): List of post objects from JSON

        Returns:
            list: List of transformed post dictionaries
        """
        extracted_posts = []

        for post in posts_data:
            try:
                # Extract post text from data array
                post_text = ""
                if post.get("data") and len(post["data"]) > 0:
                    post_text = post["data"][0].get("post", "")

                post_dict = {
                    "post_id": post.get("id", ""),
                    "timestamp": self._parse_timestamp(post.get("timestamp", "")),
                    "title": post.get("title", ""),
                    "post_texts": post_text,
                    "text_length": len(post_text)
                }

                extracted_posts.append(post_dict)

            except Exception as e:
                logger.warning(f"Failed to extract post {post.get('id', 'unknown')}: {e}")
                continue

        return extracted_posts

    def _extract_comments(self, comments_data):
        """
        Extract and transform comments from JSON data.

        Args:
            comments_data (list): List of comment objects from JSON

        Returns:
            list: List of transformed comment dictionaries
        """
        extracted_comments = []

        for comment in comments_data:
            try:
                comment_text = comment.get("comment", "")

                comment_dict = {
                    "comment_id": comment.get("id", ""),
                    "post_id": comment.get("post_id", ""),
                    "timestamp": self._parse_timestamp(comment.get("timestamp", "")),
                    "author": comment.get("author", ""),
                    "comment_texts": comment_text,
                    "text_length": len(comment_text)
                }

                extracted_comments.append(comment_dict)

            except Exception as e:
                logger.warning(f"Failed to extract comment {comment.get('id', 'unknown')}: {e}")
                continue

        return extracted_comments

    def _parse_timestamp(self, timestamp_str):
        """
        Parse timestamp string to datetime object.

        Args:
            timestamp_str (str): Timestamp string in ISO format

        Returns:
            datetime or None: Parsed datetime object or None if parsing fails
        """
        if not timestamp_str:
            return None

        try:
            # Handle ISO 8601 format with timezone
            # Example: "2024-11-18T09:42:13+0000"
            if '+' in timestamp_str or timestamp_str.endswith('Z'):
                # Remove timezone for simplicity (store as UTC)
                timestamp_str = timestamp_str.replace('Z', '+0000')
                dt = datetime.strptime(timestamp_str[:19], "%Y-%m-%dT%H:%M:%S")
                return dt
            else:
                dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                return dt

        except ValueError as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def get_posts(self):
        """Get extracted posts."""
        return self.posts

    def get_comments(self):
        """Get extracted comments."""
        return self.comments
