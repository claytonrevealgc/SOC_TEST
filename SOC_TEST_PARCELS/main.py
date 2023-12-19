import os
from datetime import datetime
import pytest
import pandas as pd
from functions import get_latest_s3_files, get_csv_from_s3, list_files_recursive

# S3 bucket name and object key prefix
BUCKET_NAME = "rgc-labstorage"
OBJECT_KEY_PREFIX = "Parcels/loveland/wkt/"

# Specify the source and destination folders
source_folder = "loveland/wkt"
destination_folder = "wkt1"


def transform_and_move_files(source_folder, destination_folder):
    os.makedirs(destination_folder, exist_ok=True)
    all_source_files = list_files_recursive(source_folder)

    for source_file in all_source_files:
        if source_file.endswith(".csv"):
            destination_file = os.path.join(
                destination_folder, os.path.basename(source_file))
            os.rename(source_file, destination_file)
            print(f"Moved file from {source_file} to {destination_file}")


def run_tests(s3_files):
    for s3_file in s3_files:
        try:
            # Load S3 data
            df = get_csv_from_s3(BUCKET_NAME, s3_file)

            # Run tests
            pytest_args = ['tests.py', '--verbose']
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            report_path = f'C:\\Users\\ndece\\Desktop\\SOC_TEST\\SOC_TEST_PARCELS\\Desktop\\SOC_PARCEL_TEST_{os.path.basename(s3_file)}_{timestamp}.html'
            pytest_args += [f'--html={report_path}', '--self-contained-html']
            pytest.main(pytest_args)

        except Exception as e:
            print(f"Error processing file {s3_file}: {e}")
            # Send notification email


# Transform and move files
transform_and_move_files(source_folder, destination_folder)

# Get the latest files in S3
s3_files = get_latest_s3_files(BUCKET_NAME, OBJECT_KEY_PREFIX)

# Run tests for each file in S3
run_tests(s3_files)
