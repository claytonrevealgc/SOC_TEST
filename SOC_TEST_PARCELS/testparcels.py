import io
import os
import pytest
import pandas as pd
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
import configparser

# Load environment variables from .env file
load_dotenv()

# Read AWS credentials from the configuration file
config = configparser.ConfigParser()
config.read('config.ini')  # Update with the correct file name

# Extract AWS credentials from the 'aws' section
aws_access_key_id = config['aws']['aws_access_key_id']
aws_secret_access_key = config['aws']['aws_secret_access_key']


def get_latest_s3_files(bucket_name, prefix):
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set as environment variables.")

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        files = []
        if 'Contents' in response:
            files = response['Contents']

        files.sort(key=lambda x: x.get('LastModified', 0), reverse=True)
        return [file['Key'] for file in files]
    except NoCredentialsError:
        raise ValueError("AWS credentials are missing or invalid.")
    except Exception as e:
        raise ValueError(f"Error listing S3 objects: {e}")


def get_csv_from_s3(bucket_name, object_key):
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set as environment variables.")

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        # Specify dtype to handle DtypeWarning
        df = pd.read_csv(io.StringIO(content), dtype=str)
        return df
    except NoCredentialsError:
        raise ValueError("AWS credentials are missing or invalid.")
    except Exception as e:
        raise ValueError(f"Error accessing S3: {e}")


# S3 bucket name and object key prefix
BUCKET_NAME = "rgc-labstorage"
OBJECT_KEY_PREFIX = "Parcels/loveland/wkt/"

# Get the latest files in S3
s3_files = get_latest_s3_files(BUCKET_NAME, OBJECT_KEY_PREFIX)

# Run tests for each file in S3
for s3_file in s3_files:
    try:
        # Load S3 data
        df = get_csv_from_s3(BUCKET_NAME, s3_file)

        # Check if the DataFrame has columns before proceeding with tests
        if df.empty or len(df.columns) == 0:
            print(
                f"Skipping tests for empty file or file with no columns: {s3_file}")
            continue

        # Test case: Format test
        def check_column_format(columns):
            assert all(
                column in df.columns for column in columns), "Some expected columns are missing."

        # Parameters for the format test
        @pytest.mark.parametrize("expected_columns", [
            ["geoid", "parcelnumb", "city", "path",
             "owner", "lat", "lon", "address",]
        ])
        def test_column_format(expected_columns):
            check_column_format(expected_columns)

        # Test case: Empty data test
        def test_empty_data():
            assert df.isnull().values.any(), "There is empty data in the file."

        # Test case: Data integrity test
        def test_data_integrity_after_conversion():
            # Convert latitude values to numbers before comparison
            df["lat"] = pd.to_numeric(df["lat"], errors="coerce")

            # Check if latitude values are within the allowed range
            assert (-90 <= df["lat"]).all() and (df["lat"] <=
                                                 90).all(), "Latitude values outside the allowed range."

        # Test case: Check for missing values after conversion
        def test_no_missing_values_after_conversion():
            assert not df["lat"].isnull().any(
            ), "There are missing values in the 'lat' column after conversion to numeric."
            assert not df["lon"].isnull().any(
            ), "There are missing values in the 'lon' column after conversion to numeric."

        # Test case: Duplicate test
        def test_duplicate():
            assert not df.duplicated().any(), "There are duplicate rows in the file."

        # Test case: Check for the absence of null values in important columns
        def test_csv_integrity():
            important_columns = ["lat", "lon"]
            assert not df[important_columns].isnull().values.any(
            ), "There are null values in important columns."

        # Test case: Date format test
        def test_date_format():
            if 'Date' in df.columns:
                assert pd.to_datetime(df['Date'], errors='coerce').notnull(
                ).all(), "There are invalid date values in the file."

        # Run tests
        pytest_args = [__file__, '--verbose']
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        report_path = f'C:\\Users\\ndece\\Desktop\\SOC_TEST_PARCELS\\Desktop\\SOC_PARCEL_TEST_{os.path.basename(s3_file)}_{timestamp}.html'
        pytest_args += [f'--html={report_path}', '--self-contained-html']
        pytest.main(pytest_args)

    except Exception as e:
        print(f"Error processing file {s3_file}: {e}")
