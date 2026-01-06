"""
Local testing script for Facebook JSON parser and transformer.

This script tests the JSON parsing and data transformation logic
without requiring S3 or database connections.
"""

import json
from src.json_parser import FacebookJSONParser
from src.data_transformer import DataTransformer
from src.logger import get_logger

logger = get_logger(__name__)


def test_json_parsing():
    """Test JSON parsing with the example facebook_file.json."""
    logger.info("=" * 80)
    logger.info("Testing JSON Parsing")
    logger.info("=" * 80)

    try:
        # Read the example JSON file
        with open('facebook_file.json', 'r') as f:
            json_content = f.read()

        logger.info(f"Loaded JSON file ({len(json_content)} bytes)")

        # Parse the JSON
        parser = FacebookJSONParser()
        posts, comments = parser.parse(json_content)

        logger.info(f"Parsed {len(posts)} posts and {len(comments)} comments")

        # Display parsed data
        logger.info("\n" + "=" * 80)
        logger.info("POSTS:")
        logger.info("=" * 80)
        for i, post in enumerate(posts, 1):
            logger.info(f"\nPost {i}:")
            logger.info(f"  ID: {post['post_id']}")
            logger.info(f"  Title: {post['title']}")
            logger.info(f"  Timestamp: {post['timestamp']}")
            logger.info(f"  Text: {post['post_texts'][:50]}..." if len(post['post_texts']) > 50 else f"  Text: {post['post_texts']}")
            logger.info(f"  Text Length: {post['text_length']}")

        logger.info("\n" + "=" * 80)
        logger.info("COMMENTS:")
        logger.info("=" * 80)
        for i, comment in enumerate(comments, 1):
            logger.info(f"\nComment {i}:")
            logger.info(f"  ID: {comment['comment_id']}")
            logger.info(f"  Post ID: {comment['post_id']}")
            logger.info(f"  Author: {comment['author']}")
            logger.info(f"  Timestamp: {comment['timestamp']}")
            logger.info(f"  Text: {comment['comment_texts'][:50]}..." if len(comment['comment_texts']) > 50 else f"  Text: {comment['comment_texts']}")
            logger.info(f"  Text Length: {comment['text_length']}")

        return posts, comments

    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise


def test_data_transformation(posts, comments):
    """Test data transformation and batching."""
    logger.info("\n" + "=" * 80)
    logger.info("Testing Data Transformation")
    logger.info("=" * 80)

    try:
        transformer = DataTransformer(batch_size=1000)

        # Validate records
        valid_posts = transformer.filter_valid_records(posts, record_type="post")
        valid_comments = transformer.filter_valid_records(comments, record_type="comment")

        logger.info(f"Valid posts: {len(valid_posts)}/{len(posts)}")
        logger.info(f"Valid comments: {len(valid_comments)}/{len(comments)}")

        # Create batches
        posts_batches = transformer.create_batches(valid_posts)
        comments_batches = transformer.create_batches(valid_comments)

        logger.info(f"Posts batches created: {len(posts_batches)}")
        logger.info(f"Comments batches created: {len(comments_batches)}")

        # Show batch details
        for i, batch in enumerate(posts_batches, 1):
            logger.info(f"  Posts Batch {i}: {len(batch)} records")

        for i, batch in enumerate(comments_batches, 1):
            logger.info(f"  Comments Batch {i}: {len(batch)} records")

        return posts_batches, comments_batches

    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise


def main():
    """Run all local tests."""
    try:
        # Test 1: JSON Parsing
        posts, comments = test_json_parsing()

        # Test 2: Data Transformation
        posts_batches, comments_batches = test_data_transformation(posts, comments)

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("=" * 80)
        logger.info("✓ JSON parsing successful")
        logger.info("✓ Data validation successful")
        logger.info("✓ Batch creation successful")
        logger.info("\nAll tests passed!")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("TESTS FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
