import json
import os
import boto3
from datetime import date, timedelta
from ftplib import FTP
from botocore.exceptions import ClientError

def ftp_connection():
    try:
        host = os.environ.get('FTP_HOST')
        username = os.environ.get('FTP_USERNAME')
        password = os.environ.get('FTP_PASSWORD')
        print(host, username, password)
        ftp = FTP(host)
        ftp.login(username, password)
        print('Successfully connected to FTP server')
        return ftp
    except Exception as e:
        print(f"Failed to connect to FTP server: {str(e)}")
        raise

def directory_string(date_obj):
    return f"/v3/{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}/CSV"

def ensure_s3_directory(bucket_name, directory_path, s3_client):
    try:
        subdirectories = directory_path.strip('/').split('/')[1:]
        print(subdirectories)
        cur_path = ''
        for i, sub in enumerate(subdirectories):
            if cur_path:
                cur_path += f"/{sub}"
            else:
                cur_path = sub

            dir_path = f'{cur_path}/' # trailing slash to indicate directory

            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=dir_path,
                    MaxKeys=1
                )
                dir_exists = response['KeyCount'] > 0
                if dir_exists:
                    print(f"Directory {dir_path} already exists in {bucket_name}.")
                else:
                    print(f"Creating new directory {dir_path} in {bucket_name}...")
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=dir_path
                    )
                    print(f"Successfully created new directory {dir_path} in {bucket_name}.")
            except ClientError as e:
                print(f"Error checking/creating directory {dir_path}: {e}")
                raise


        # Check if director
    except ClientError as e:
        print(f"Error connecting to bucket {bucket_name}: {e}")
        raise

def lambda_handler(event, context):
    # Connect to FTP server
    ftp = ftp_connection()
    # Get folder name based on date
    yesterday = date.today() - timedelta(days=200)
    directory = directory_string(yesterday)
    print(directory)
    # Change directory
    ftp.cwd(directory)
    # Get all files from that directory
    files = []
    ftp.retrlines('NLST', files.append)
    print(f"Found {len(files)} files in directory")
    print(files)
    # Upload to S3
    s3_client = boto3.client('s3')
    ensure_s3_directory('alpb-ftp-test', directory, s3_client)
    pass

lambda_handler(None, None)
