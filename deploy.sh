#!/bin/bash

# Deployment script for AWS Lambda
# This script creates a deployment package with all dependencies

echo "=========================================="
echo "Creating Lambda Deployment Package"
echo "=========================================="

# Clean up previous build
echo "Cleaning up previous builds..."
rm -rf package
rm -f lambda-deployment.zip

# Create package directory
echo "Creating package directory..."
mkdir -p package

# Install dependencies to package directory
echo "Installing dependencies..."
pip install -r requirements.txt -t package/

# Copy source code to package
echo "Copying source code..."
cp -r src package/
cp lambda_function.py package/

# Copy config (if needed in package)
cp -r config package/ 2>/dev/null || echo "No config directory to copy"

# Create deployment zip
echo "Creating deployment zip file..."
cd package
zip -r ../lambda-deployment.zip . -q
cd ..

# Check zip size
ZIP_SIZE=$(du -h lambda-deployment.zip | cut -f1)
echo "=========================================="
echo "Deployment package created: lambda-deployment.zip"
echo "Package size: $ZIP_SIZE"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Upload lambda-deployment.zip to AWS Lambda"
echo "2. Set handler to: lambda_function.lambda_handler"
echo "3. Configure environment variables for database credentials"
echo "4. Set memory to 1024 MB and timeout to 10 minutes"
echo "=========================================="
