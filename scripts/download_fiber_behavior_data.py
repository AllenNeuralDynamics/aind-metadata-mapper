"""
Script to download CSV files from a given S3 URI to local data folders.

Usage:
    python download_fiber_behaviordata.py S3_URI [--profile PROFILE_NAME]

Example:
    python scripts/download_fiber_behavior_data.py s3://aind-private-data-prod-o5171v/behavior_694360_2024-01-10_13-43-17 # noqa: E501
    python scripts/download_fiber_behavior_data.py s3://aind-private-data-prod-o5171v/behavior_694360_2024-01-10_13-43-17 --profile my-aws-profile # noqa: E501

AWS Credentials Setup:
--------------------
This script requires AWS credentials to be configured.
You can set them up in two ways:

1. Using AWS CLI (recommended):
   ```bash
   # Install AWS CLI if you haven't already
   pip install awscli

   # Configure credentials
   aws configure --profile aind-prod
   ```
   When prompted, enter your:
   - AWS Access Key ID
   - AWS Secret Access Key
   - Default region (e.g., us-west-2)
   - Default output format (json)

2. Manually creating credentials file:
   ```bash
   # Create AWS credentials directory
   mkdir -p ~/.aws

   # Create/edit credentials file
   nano ~/.aws/credentials
   ```
   Add the following content:
   ```
   [aind-prod]
   aws_access_key_id = YOUR_ACCESS_KEY
   aws_secret_access_key = YOUR_SECRET_KEY
   ```

The script will look for credentials under the specified
profile (defaults to 'aind-prod' if not specified).

Data Storage:
------------
Files will be downloaded to a 'data' directory in the root of this repository.
The directory structure will be:
    data/
      session_name/
        behavior/
          *.csv
        fib/
          *.csv
"""

import argparse
import logging
from pathlib import Path
import s3fs
import sys


def get_repo_root():
    """Get the root directory of the repository"""
    # Start from the directory containing this script
    current_dir = Path(__file__).resolve().parent
    # Go up until we find the repository root (where .git directory is)
    while current_dir.parent != current_dir:  # Stop at root directory
        if (current_dir / ".git").exists():
            return current_dir
        current_dir = current_dir.parent
    raise RuntimeError("Could not find repository root directory")


def get_s3fs(profile_name="aind-prod"):
    """Get an s3fs filesystem object configured with the right credentials"""
    return s3fs.S3FileSystem(profile=profile_name)


def download_session_data(s3_uri, profile_name="aind-prod"):
    """
    Download CSV files from a given S3 URI to local data folders.

    Args:
        s3_uri (str): The S3 URI of the session
        (e.g., s3://bucket/path/to/session)
        profile_name (str): The AWS profile name to use for credentials

    Returns:
        bool: True if successful, False otherwise
    """
    if not s3_uri.startswith("s3://"):
        logging.error("S3 URI must start with 's3://'")
        return False

    # Remove 's3://' and split into bucket and prefix
    path = s3_uri[5:]
    bucket_name = path.split("/")[0]
    prefix = "/".join(path.split("/")[1:])

    # Get the session name from the prefix
    session_name = prefix.split("/")[-1]
    logging.info(f"Processing session: {session_name}")

    # Get s3fs filesystem
    fs = get_s3fs(profile_name)

    # Create data directory structure in repo root
    repo_root = get_repo_root()
    data_dir = repo_root / "data" / session_name
    behavior_dir = data_dir / "behavior"
    fib_dir = data_dir / "fib"

    # Create directories
    behavior_dir.mkdir(parents=True, exist_ok=True)
    fib_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Created directory structure at: {data_dir}")

    # Download CSV files from behavior directory
    behavior_path = f"{bucket_name}/{prefix}/behavior"
    try:
        behavior_files = fs.ls(behavior_path)
        for file in behavior_files:
            if file.lower().endswith(".csv"):
                local_path = behavior_dir / Path(file).name
                logging.info(f"Downloading {file} to {local_path}")
                fs.get(file, str(local_path))
    except Exception as e:
        logging.error(f"Error downloading behavior files: {e}")
        return False

    # Download CSV files from fib directory
    fib_path = f"{bucket_name}/{prefix}/fib"
    try:
        fib_files = fs.ls(fib_path)
        for file in fib_files:
            if file.lower().endswith(".csv"):
                local_path = fib_dir / Path(file).name
                logging.info(f"Downloading {file} to {local_path}")
                fs.get(file, str(local_path))
    except Exception as e:
        logging.error(f"Error downloading fib files: {e}")
        return False

    # Print final location clearly
    print("\n" + "=" * 80)
    print("Files have been downloaded to:")
    print(f"    {data_dir}")
    print("=" * 80 + "\n")

    return True


def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Download CSV files from a given S3 URI"
    )
    parser.add_argument(
        "s3_uri",
        help="The S3 URI of the session (e.g., s3://bucket/path/to/session)",
    )
    parser.add_argument(
        "--profile",
        default="aind-prod",
        help="AWS profile name to use for credentials (default: aind-prod)",
    )
    args = parser.parse_args()

    if not download_session_data(args.s3_uri, args.profile):
        sys.exit(1)


if __name__ == "__main__":
    main()
