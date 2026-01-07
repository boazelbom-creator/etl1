"""
Cross-platform deployment script for AWS Lambda.
Creates a deployment package with all dependencies.
"""

import os
import shutil
import subprocess
import zipfile
from pathlib import Path


def clean_previous_builds():
    """Remove previous build artifacts."""
    print("Cleaning up previous builds...")
    if os.path.exists("package"):
        shutil.rmtree("package")
    if os.path.exists("lambda-deployment.zip"):
        os.remove("lambda-deployment.zip")


def create_package_directory():
    """Create package directory for deployment."""
    print("Creating package directory...")
    os.makedirs("package", exist_ok=True)


def install_dependencies():
    """Install Python dependencies to package directory."""
    print("Installing dependencies...")
    try:
        subprocess.run(
            ["pip", "install", "-r", "requirements.txt", "-t", "package/"],
            check=True,
            capture_output=True,
            text=True
        )
        print("✓ Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"✗ Error installing dependencies: {e}")
        print(e.stderr)
        raise


def copy_source_code():
    """Copy source code to package directory."""
    print("Copying source code...")

    # Copy src directory
    if os.path.exists("src"):
        shutil.copytree("src", "package/src", dirs_exist_ok=True)

    # Copy lambda_function.py
    shutil.copy2("lambda_function.py", "package/")

    # Copy config directory if it exists
    if os.path.exists("config"):
        shutil.copytree("config", "package/config", dirs_exist_ok=True)

    print("✓ Source code copied")


def create_deployment_zip():
    """Create deployment zip file."""
    print("Creating deployment zip file...")

    with zipfile.ZipFile("lambda-deployment.zip", "w", zipfile.ZIP_DEFLATED) as zipf:
        # Walk through package directory and add all files
        for root, dirs, files in os.walk("package"):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, "package")
                zipf.write(file_path, arcname)

    print("✓ Deployment zip created")


def get_zip_size():
    """Get size of deployment zip."""
    size_bytes = os.path.getsize("lambda-deployment.zip")
    size_mb = size_bytes / (1024 * 1024)
    return size_mb


def main():
    """Main deployment function."""
    print("=" * 60)
    print("Creating Lambda Deployment Package")
    print("=" * 60)
    print()

    try:
        clean_previous_builds()
        create_package_directory()
        install_dependencies()
        copy_source_code()
        create_deployment_zip()

        size_mb = get_zip_size()

        print()
        print("=" * 60)
        print(f"✓ Deployment package created: lambda-deployment.zip")
        print(f"  Package size: {size_mb:.2f} MB")
        print("=" * 60)
        print()
        print("Next steps:")
        print("1. Upload lambda-deployment.zip to AWS Lambda")
        print("2. Set handler to: lambda_function.lambda_handler")
        print("3. Configure environment variables:")
        print("   - DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT")
        print("   - S3_BUCKET, S3_FOLDER, S3_FILE")
        print("4. Set memory to 1024 MB and timeout to 10 minutes (600 seconds)")
        print("5. Ensure Lambda has:")
        print("   - S3 read permissions (IAM role)")
        print("   - VPC access if database is in VPC")
        print("=" * 60)

        if size_mb > 50:
            print()
            print("⚠ WARNING: Package size exceeds 50MB!")
            print("  Consider using AWS Lambda Layers for dependencies")

    except Exception as e:
        print()
        print("=" * 60)
        print(f"✗ Deployment failed: {e}")
        print("=" * 60)
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
