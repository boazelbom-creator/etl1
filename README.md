# Facebook JSON to PostgreSQL ETL Lambda Function

AWS Lambda function that extracts Facebook post and comment data from JSON files stored in S3, transforms the data, and loads it into PostgreSQL tables.

## Features

- **Batch Processing**: Processes records in batches of 1000 for memory efficiency
- **UPSERT Logic**: Handles duplicate IDs gracefully using `INSERT ... ON CONFLICT DO UPDATE`
- **Resumability**: Safe to re-run if process fails mid-execution
- **Error Handling**: Continues processing remaining batches even if some fail
- **Comprehensive Logging**: Detailed CloudWatch logs for monitoring and debugging
- **Scalable**: Designed to handle ~100,000 posts per execution

## Project Structure

```
etl1/
├── lambda_function.py          # Main Lambda handler
├── requirements.txt            # Python dependencies
├── schema.sql                  # PostgreSQL schema
├── PRD.md                      # Product requirements document
├── facebook_file.json          # Example JSON structure
├── config/
│   └── config.json            # Configuration template
├── security/                   # Placeholder for credentials (not in git)
└── src/
    ├── __init__.py
    ├── config_manager.py      # Configuration loading and validation
    ├── s3_reader.py          # S3 file reading
    ├── json_parser.py        # Facebook JSON parsing
    ├── data_transformer.py   # Data validation and batching
    ├── database_writer.py    # PostgreSQL batch UPSERT operations
    └── logger.py             # Centralized logging
```

## Prerequisites

1. **Python 3.12** with virtual environment
2. **PostgreSQL Database** (accessible from Lambda)
3. **AWS Account** with:
   - S3 bucket with Facebook JSON files
   - Lambda execution role with S3 read permissions
   - VPC configuration (if database is in VPC)
   - **VPC Endpoint for S3** (required if Lambda is in VPC)

## Setup Instructions

### 1. Database Setup

Run the schema creation script on your PostgreSQL database:

```bash
psql -h your-host -U your-username -d your-database -f schema.sql
```

This creates:
- `posts` table
- `comments` table with foreign key to posts
- Indexes for performance

### 2. Local Development Setup

Create and activate virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### 3. Configuration

Edit `config/config.json` with your settings:

```json
{
  "s3": {
    "bucket_name": "your-bucket-name",
    "folder_path": "path/to/folder",
    "file_name": "facebook_file.json"
  },
  "database": {
    "host": "your-postgres-host.amazonaws.com",
    "database": "your-database-name",
    "username": "your-username",
    "password": "your-password",
    "port": 5432
  },
  "batch_size": 1000
}
```

**Security Note**: For production, use AWS Secrets Manager or environment variables instead of storing credentials in config files.

### 4. Local Testing

Test the Lambda function locally:

```bash
python lambda_function.py
```

**Note**: For local testing, you'll need:
- AWS credentials configured (`aws configure`)
- Network access to your PostgreSQL database
- S3 bucket with test data

## Deployment to AWS Lambda

### 1. Create Deployment Package

Use the automated deployment script:

```bash
python3 deploy.py
```

This script will:
- Download Lambda-compatible psycopg2 for Python 3.12
- Install all dependencies
- Copy source code
- Create `lambda-deployment.zip` package

**Manual deployment (alternative):**
```bash
# Clean previous builds
rm -rf package lambda-deployment.zip

# Create package directory
mkdir package

# Download Lambda-compatible psycopg2
pip3 download psycopg2-binary==2.9.9 --platform manylinux2014_x86_64 \
  --only-binary=:all: --python-version 312 --implementation cp \
  --abi cp312 -d psycopg2_temp
cd psycopg2_temp && unzip *.whl && cd ..

# Install other dependencies
pip3 install boto3 python-dotenv -t package/

# Copy psycopg2 and source code
cp -r psycopg2_temp/psycopg2 psycopg2_temp/psycopg2_binary.libs package/
cp -r src package/
cp lambda_function.py package/

# Create ZIP (use Python if zip command not available)
cd package && zip -r ../lambda-deployment.zip . && cd ..
```

### 2. Create Lambda Function

Using AWS CLI:

```bash
aws lambda create-function \
  --function-name facebook-json-to-postgres-etl \
  --runtime python3.12 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/lambda-execution-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://lambda-deployment.zip \
  --timeout 600 \
  --memory-size 1024 \
  --vpc-config SubnetIds=subnet-xxx,SecurityGroupIds=sg-xxx
```

Or update existing function:

```bash
aws lambda update-function-code \
  --function-name facebook-json-to-postgres-etl \
  --zip-file fileb://lambda-deployment.zip \
  --region YOUR_REGION
```

### 3. Configure Environment Variables

Set environment variables in Lambda console or via CLI:

```bash
aws lambda update-function-configuration \
  --function-name facebook-json-to-postgres-etl \
  --environment Variables="{
    DB_HOST=your-host,
    DB_NAME=your-database,
    DB_USERNAME=your-username,
    DB_PASSWORD=your-password,
    DB_PORT=5432,
    S3_BUCKET_NAME=your-bucket,
    S3_FOLDER_PATH=path/to/folder,
    S3_FILE_NAME=facebook_file.json,
    BATCH_SIZE=1000
  }"
```

### 4. VPC Endpoint Setup (Required if Lambda is in VPC)

If your Lambda function is deployed in a VPC, create an S3 VPC Endpoint:

1. Go to **VPC Console** → **Endpoints** → **Create endpoint**
2. **Service category**: AWS services
3. **Services**: Select `com.amazonaws.YOUR-REGION.s3` (Type: Gateway)
4. **VPC**: Select the same VPC as your Lambda
5. **Route tables**: Select all route tables for Lambda's subnets
6. **Policy**: Full access
7. Click **Create endpoint**

**Note**: Gateway VPC Endpoints for S3 are free and required for Lambda to access S3 from within a VPC.

### 5. IAM Role Permissions

Ensure your Lambda execution role has:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface"
      ],
      "Resource": "*"
    }
  ]
}
```

## Usage

### Invoke Lambda Manually

```bash
aws lambda invoke \
  --function-name facebook-json-to-postgres-etl \
  --payload '{}' \
  response.json

cat response.json
```

### S3 Trigger (Optional)

Configure S3 to trigger Lambda automatically when new files are uploaded:

1. Go to Lambda console → Add trigger → S3
2. Select your bucket
3. Event type: PUT
4. Prefix: `path/to/folder/` (optional)
5. Suffix: `.json` (optional)

## Response Format

Successful execution returns:

```json
{
  "statusCode": 200,
  "body": {
    "message": "ETL process completed successfully",
    "posts": {
      "total": 100000,
      "batches": 100,
      "success": 100000,
      "failed": 0
    },
    "comments": {
      "total": 50000,
      "batches": 50,
      "success": 50000,
      "failed": 0
    }
  }
}
```

## Monitoring

### CloudWatch Logs

View logs in AWS CloudWatch:
- Log group: `/aws/lambda/facebook-json-to-postgres-etl`
- Each execution creates detailed logs with:
  - Batch processing progress
  - Success/failure counts
  - Error messages with context

### Key Metrics to Monitor

- Lambda duration (should be < 600 seconds)
- Memory usage (configured for 1024MB)
- Error rate
- Database connection issues

## Troubleshooting

### Common Issues

1. **Timeout Error**
   - Increase Lambda timeout (max 15 minutes)
   - Reduce batch size in config
   - Check database performance

2. **Memory Error**
   - Increase Lambda memory (up to 10GB)
   - Implement streaming for very large files

3. **Database Connection Failed**
   - Verify VPC configuration
   - Check security group rules
   - Verify database credentials

4. **S3 Timeout Error (Lambda in VPC)**
   - Create S3 VPC Endpoint (Gateway type)
   - Verify endpoint is attached to Lambda's route tables
   - Check S3 bucket permissions

5. **Foreign Key Constraint Error**
   - Ensure posts are inserted before comments
   - Verify post_id references are valid

6. **Duplicate Key Error**
   - This should not occur with UPSERT logic
   - Check that primary keys are correctly specified

7. **psycopg2 Import Error**
   - Use the `deploy.py` script to ensure Lambda-compatible psycopg2
   - Verify Python runtime is set to 3.12
   - Do not use local pip install for psycopg2 (architecture mismatch)

## Data Model

### Posts Table
- `post_id` (PK): Unique post identifier
- `timestamp`: Post creation time
- `title`: Post title
- `post_texts`: Post content
- `text_length`: Character count of post content

### Comments Table
- `comment_id` (PK): Unique comment identifier
- `post_id` (FK): Reference to posts table
- `timestamp`: Comment creation time
- `author`: Comment author name
- `comment_texts`: Comment content
- `text_length`: Character count of comment content

## Performance Considerations

- **Batch Size**: Default 1000 records per batch (configurable)
- **Expected Volume**: ~100,000 posts per execution
- **Execution Time**: ~5-10 minutes for 100k posts
- **Memory Usage**: ~500-800 MB for typical workloads
- **Database Load**: Batched inserts minimize connection overhead

## Security Best Practices

1. **Never commit credentials** to version control
2. Use **AWS Secrets Manager** for production credentials
3. Use **IAM roles** with least privilege
4. Enable **encryption at rest** for S3 and RDS
5. Use **SSL/TLS** for database connections
6. Regularly **rotate credentials**
7. Enable **CloudTrail** logging for audit

## Future Enhancements

- Incremental processing (only new/updated posts)
- Dead letter queue for failed records
- Step Functions orchestration for large files
- Automated testing suite
- Performance metrics dashboard
- Automated schema migrations

## Support

For issues and questions:
- Check CloudWatch logs for detailed error messages
- Review PRD.md for requirements clarification
- Verify database schema matches schema.sql

## Implementation Notes

### Deployment History

**Version 1.0 (2026-01-07)**
- ✅ Implemented with Python 3.12 runtime
- ✅ Automated deployment via `deploy.py` script
- ✅ Lambda-compatible psycopg2 binary packaging
- ✅ VPC Endpoint configuration for S3 access
- ✅ Successfully deployed and tested
- ✅ Processing ~100,000 posts with batch UPSERT

### Key Learnings

1. **psycopg2 Compatibility**: Must use Lambda-compatible binaries (manylinux2014_x86_64) downloaded specifically for target Python version
2. **VPC Networking**: Lambda in VPC requires S3 VPC Endpoint (Gateway) for S3 access - no NAT Gateway needed
3. **Environment Variables**: Code reads from environment variables when config file not present
4. **Deployment Package**: Use automated `deploy.py` script to ensure correct binary compatibility

## License

Internal use only - Kaplan project
