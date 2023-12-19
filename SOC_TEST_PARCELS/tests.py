import os
import pandas as pd
import pytest
from functions import get_csv_from_s3, get_latest_s3_files, list_files_recursive

# S3 bucket name and object key prefix
BUCKET_NAME = "rgc-labstorage"
OBJECT_KEY_PREFIX = "Parcels/loveland/wkt/"

# Specify the source and destination folders
source_folder = "loveland/wkt"
destination_folder = "wkt1"

# Transform folder structure
os.makedirs(destination_folder, exist_ok=True)
all_source_files = list_files_recursive(source_folder)

# Move files to the destination folder
for source_file in all_source_files:
    if source_file.endswith(".csv"):
        destination_file = os.path.join(
            destination_folder, os.path.basename(source_file))
        os.rename(source_file, destination_file)
        print(f"Moved file from {source_file} to {destination_file}")

# Get the latest files in S3
s3_files = get_latest_s3_files(BUCKET_NAME, OBJECT_KEY_PREFIX)

# Load S3 data for testing
df = get_csv_from_s3(BUCKET_NAME, s3_files[0])  # Adjust index if needed

# Test case: Format test


def check_column_format(columns):
    assert all(
        column in df.columns for column in columns), "Some expected columns are missing."

# Parameters for the format test


@pytest.mark.parametrize("expected_columns", [
    ["geoid", "parcelnumb", "city", "path", "owner", "lat", "lon", "address"]
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
