import os
import boto3
import traceback
from datetime import date, timedelta
from ftplib import FTP
from botocore.exceptions import ClientError
from io import BytesIO

def ftp_connection():
    try:
        host = os.environ.get('FTP_HOST')
        username = os.environ.get('FTP_USERNAME')
        password = os.environ.get('FTP_PASSWORD')
        ftp = FTP(host)
        ftp.login(username, password)
        print('Successfully connected to FTP server')
        return ftp
    except Exception as e:
        print(f"Failed to connect to FTP server: {str(e)}")
        raise e

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
                raise e


        # Check if director
    except ClientError as e:
        print(f"Error connecting to bucket {bucket_name}: {e}")
        raise e

def create_s3_key(directory, filename):
    new_directory = directory[4:] # remove '/v3/' from start of string
    new_directory += f'/{filename}'
    return new_directory

def ftp_to_s3(ftp, ftp_directory, filename, s3_client, bucket_name, s3_key):
    try:
        if s3_obj_exists(s3_client, bucket_name, s3_key):
            return
        file_data = BytesIO()
        print(f'Downloading {filename} from {ftp_directory} on FTP server...')
        ftp.retrbinary(f'RETR {filename}', file_data.write)
        file_data.seek(0) # reset internal file pointer

        print(f'Uploading {s3_key} to S3 bucket {bucket_name}...')
        s3_client.upload_fileobj(file_data, bucket_name, s3_key)
        print('Upload successful.')
    except Exception as e:
        print(f'Error during transfer from FTP to S3: {str(e)}')
        raise e
    
def s3_obj_exists(s3_client, bucket_name, s3_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        print(f'Object {s3_key} already exists in bucket {bucket_name}.')
        return True
    except ClientError as e:
        # If the object doesn't exist, AWS returns a 404 error
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # If it was a different error (permissions, etc.), re-raise it
            print(f"Error checking for object: {e}")
            raise

def lambda_handler(event, context):
    try:
        # Connect to FTP server
        ftp = ftp_connection()
        # Get folder name based on date
        days_ago = int(os.environ.get('DAYS_AGO', 1))
        yesterday = date.today() - timedelta(days=days_ago)
        ftp_directory = directory_string(yesterday)
        # Change directory
        ftp.cwd(ftp_directory)
        # Get all files from that directory
        filenames = []
        ftp.retrlines('NLST', filenames.append)
        print(f"Found {len(filenames)} files in FTP directory for {yesterday}.")
        # Upload to S3
        s3_client = boto3.client('s3')
        bucket_name = os.environ.get('BUCKET_NAME', 'alpb-ftp-test')
        ensure_s3_directory(bucket_name, ftp_directory, s3_client)
        for filename in filenames:
            s3_key = create_s3_key(ftp_directory, filename)
            ftp_to_s3(ftp, ftp_directory, filename, s3_client, bucket_name, s3_key)
        print(f'All files uploaded to FTP for {yesterday} successfully transfered to S3!')
    except Exception as e:
        print(f'Error during FTP job: {str(e)}')
        print('Traceback:')
        print('********************************************************')
        print(traceback.format_exc())
        print('********************************************************')
    finally:
        ftp.quit()
