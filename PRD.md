# Product Requirements Document (PRD)
## Facebook JSON to PostgreSQL ETL Lambda Function

---

## 1. Project Overview

### 1.1 Purpose
Build an AWS Lambda function that extracts Facebook post and comment data from JSON files stored in S3, transforms the data, and loads it into a PostgreSQL database.

### 1.2 Scope
This Lambda function will process Facebook export data in JSON format, parse posts and comments, calculate text metrics, and store structured data in PostgreSQL tables for further analysis.

---

## 2. Functional Requirements

### 2.1 S3 Integration
- **FR-1.1**: Lambda function must connect to a specified S3 bucket and folder
- **FR-1.2**: Read JSON files from the S3 location
- **FR-1.3**: Support configurable S3 bucket and file path through configuration file

### 2.2 Data Processing
- **FR-2.1**: Parse Facebook JSON format containing posts, comments, and metadata (see `facebook_file.json` for structure)
- **FR-2.2**: Extract the following data elements:
  - Posts: `id`, `title`, `timestamp`, post text from `data[0].post`
  - Comments: `id`, `post_id`, `timestamp`, `author`, comment text from `comment` field
- **FR-2.3**: Calculate text length for both posts and comments
- **FR-2.4**: Ignore/skip attachment data from the Facebook export

### 2.3 Data Transformation
- **FR-3.1**: Transform nested Facebook JSON structure into flat relational tables
- **FR-3.2**: Use provided `id` fields from JSON as primary keys for posts and comments
- **FR-3.3**: Maintain post-comment relationships through foreign keys (`post_id`)
- **FR-3.4**: Process records in batches of 1000 rows for memory efficiency
- **FR-3.5**: Use UPSERT logic (INSERT ... ON CONFLICT) to handle duplicate IDs on re-runs

### 2.4 Database Storage

#### Posts Table Schema
- `post_id` (Primary Key)
- `timestamp` (DateTime)
- `title` (Text)
- `post_texts` (Text) - extracted from `data[0].post`
- `text_length` (Integer)

#### Comments Table Schema
- `comment_id` (Primary Key)
- `post_id` (Foreign Key referencing posts.post_id)
- `timestamp` (DateTime)
- `author` (Text)
- `comment_texts` (Text) - extracted from `comment` field
- `text_length` (Integer)

### 2.5 Error Handling
- **FR-4.1**: Handle malformed JSON gracefully
- **FR-4.2**: Log processing errors with context
- **FR-4.3**: Continue processing remaining records if individual records fail
- **FR-4.4**: Return summary of successful and failed operations
- **FR-4.5**: Use database transactions per batch - commit every 1000 records
- **FR-4.6**: On batch failure, log failed batch range and continue with next batch
- **FR-4.7**: Track progress (last successfully committed batch) for resumability

---

## 3. Non-Functional Requirements

### 3.1 Technology Stack
- **NFR-1.1**: Implementation language: Python 3.x
- **NFR-1.2**: Use Python virtual environment for dependency management
- **NFR-1.3**: Target runtime: AWS Lambda
- **NFR-1.4**: Database: PostgreSQL

### 3.2 Configuration Management
- **NFR-2.1**: Configuration file must include:
  - PostgreSQL database URL
  - Database credentials (username, password, database name)
  - S3 bucket name and path
  - JSON file name
- **NFR-2.2**: Separate security credentials from application configuration
- **NFR-2.3**: AWS API keys stored in dedicated security folder (not in code)

### 3.3 Security
- **NFR-3.1**: Database credentials must not be hardcoded
- **NFR-3.2**: AWS credentials stored securely (AWS Secrets Manager or environment variables)
- **NFR-3.3**: Follow principle of least privilege for IAM roles
- **NFR-3.4**: Encrypt sensitive data in transit and at rest

### 3.4 Performance
- **NFR-4.1**: Lambda configuration: 1024MB memory, 10 minute timeout (processing ~100,000 posts)
- **NFR-4.2**: Process files efficiently within Lambda timeout constraints
- **NFR-4.3**: Use batch inserts (1000 records per commit) for database operations
- **NFR-4.4**: Optimize memory usage for large JSON files (~100,000 posts expected)
- **NFR-4.5**: Stream JSON parsing if file size exceeds memory limits

### 3.5 Maintainability
- **NFR-5.1**: Code must be modular and well-documented
- **NFR-5.2**: Use virtual environment with requirements.txt for dependencies
- **NFR-5.3**: Follow Python PEP 8 style guidelines
- **NFR-5.4**: Include logging for debugging and monitoring

---

## 4. Technical Architecture

### 4.1 Components
1. **S3 Reader Module**: Connects to S3 and retrieves JSON file
2. **JSON Parser Module**: Parses Facebook JSON format and extracts relevant data
3. **Data Transformer Module**: Converts nested structures to flat records and calculates metrics
4. **Database Writer Module**: Handles PostgreSQL connection and data insertion
5. **Configuration Manager**: Loads and validates configuration parameters
6. **Logger**: Centralized logging for all operations

### 4.2 Data Flow
```
S3 Bucket → Lambda Trigger → Read JSON → Parse Data → Batch (1000 records) →
UPSERT to PostgreSQL (per batch) → Commit → Next Batch → Complete
```

**Batch Processing Flow:**
1. Read entire JSON from S3
2. Extract all posts and comments
3. Split into batches of 1000 records
4. For each batch:
   - Begin transaction
   - Execute UPSERT (INSERT ... ON CONFLICT DO UPDATE)
   - Commit transaction
   - Log batch completion
5. Continue with next batch even if previous batch fails
6. Return summary of all batches processed

### 4.3 AWS Services
- **AWS Lambda**: Compute service for running the function
- **Amazon S3**: Source data storage
- **AWS Secrets Manager** (recommended): Secure credential storage
- **CloudWatch Logs**: Logging and monitoring

### 4.4 Dependencies (Estimated)
- `boto3`: AWS SDK for S3 operations
- `psycopg2-binary`: PostgreSQL database adapter
- `python-dotenv` or similar: Configuration management

---

## 5. Data Model

### 5.1 Facebook JSON Structure (Input)
See `facebook_file.json` for example structure:
- Top-level `posts` array containing post objects
- Top-level `comments` array containing comment objects
- Posts contain: `id`, `timestamp`, `title`, `data` array with `post` text, `attachments` (ignored)
- Comments contain: `id`, `post_id`, `timestamp`, `author`, `comment` text
- Comments reference posts via `post_id` field

### 5.2 PostgreSQL Schema (Output)

**Table: posts**
```sql
CREATE TABLE posts (
    post_id VARCHAR(255) PRIMARY KEY,
    timestamp TIMESTAMP,
    title TEXT,
    post_texts TEXT,
    text_length INTEGER
);
```

**Table: comments**
```sql
CREATE TABLE comments (
    comment_id VARCHAR(255) PRIMARY KEY,
    post_id VARCHAR(255) REFERENCES posts(post_id),
    timestamp TIMESTAMP,
    author VARCHAR(255),
    comment_texts TEXT,
    text_length INTEGER
);
```

### 5.3 Duplicate Handling Strategy
- Use PostgreSQL `INSERT ... ON CONFLICT (primary_key) DO UPDATE` for upsert operations
- On duplicate `post_id` or `comment_id`, update all fields with new values
- This allows safe re-runs if the process fails mid-execution
- Ensures idempotency - running the same file multiple times produces the same result

---

## 6. Configuration Structure

### 6.1 Configuration File (config.json/config.yaml)
```
{
  "s3": {
    "bucket_name": "<bucket-name>",
    "folder_path": "<folder-path>",
    "file_name": "<json-file-name>"
  },
  "database": {
    "url": "<postgres-url>",
    "database": "<db-name>",
    "username": "<db-username>",
    "password": "<db-password>",
    "port": 5432
  }
}
```

### 6.2 Security Folder Structure
```
/security/
  - aws_credentials.json (AWS API keys)
  - .env (environment variables)
```

---

## 7. Assumptions

1. Facebook JSON file follows standard Facebook export format (see `facebook_file.json`)
2. PostgreSQL database and tables are pre-created or function has DDL permissions
3. Lambda function has appropriate IAM role with S3 read permissions
4. Network connectivity exists between Lambda and PostgreSQL (VPC configuration if needed)
5. Expected data volume: ~100,000 posts per file
6. One JSON file processed per Lambda invocation
7. Lambda has sufficient memory (1024MB) and timeout (10 minutes) for processing
8. Database can handle batch inserts of 1000 records efficiently
9. Re-running the same file is safe due to UPSERT logic

---

## 8. Out of Scope

1. Database schema creation/migration scripts
2. S3 bucket creation and configuration
3. Multiple file processing in single invocation
4. Attachment data processing
5. Data validation beyond basic type checking
6. Incremental updates (assumes full load)
7. Lambda deployment automation (SAM/Terraform/CDK)
8. Monitoring dashboards and alerts

---

## 9. Success Criteria

1. Lambda function successfully reads JSON from specified S3 location
2. All posts and comments are correctly parsed and stored in respective tables
3. Text length calculations are accurate
4. Batch processing commits every 1000 records successfully
5. UPSERT logic prevents duplicate entries on re-runs
6. Function can process ~100,000 posts within timeout limits
7. Failed batches are logged without stopping entire process
8. No sensitive credentials in code
9. Proper error logging for troubleshooting
10. Virtual environment with all dependencies documented

---

## 10. Future Enhancements (Optional)

1. Support for incremental processing (only new/updated posts)
2. Support for batch file processing
3. Data validation and cleansing rules
4. Duplicate detection and handling
5. Automated database schema creation
6. Integration with AWS Step Functions for orchestration
7. Dead letter queue for failed processing
8. Metrics and monitoring dashboards

---

**Document Version**: 1.2
**Last Updated**: 2026-01-06
**Status**: Final - Ready for Implementation
**Changes in v1.2**: Added batch processing (1000 records/commit), UPSERT duplicate handling, scaled for ~100k posts
