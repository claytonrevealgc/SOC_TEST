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

        files = response.get('Contents', [])
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


def list_files_recursive(parent_folder):
    all_files = []
    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)
    return all_files
