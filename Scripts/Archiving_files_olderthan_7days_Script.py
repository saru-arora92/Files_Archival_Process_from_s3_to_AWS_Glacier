import logging
import boto3
import datetime
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    bucket_name = 'bucket_name'
    s3 = boto3.client('s3')

    objects = list_bucket_objects(bucket_name)
    if objects is not None:
        # List the object names
        logging.info(f'Objects in {bucket_name}')
        count = len(objects)
        print(f'Backup count: {count}')
        last = '0'

        for obj in objects:
            if count <= 56:  # For not deleting from the last 7 days! (Backup every 3 hours per day, 24/3 * 7 = 56)
                return True

            count = count - 1  # For not deleting from the last 7 days!
            print(f'{count}')

            # Start Policy for everything older then one week
            date = obj["Key"].replace('prefix_before_time', '')
            dateArray = date.split("T")
            date = dateArray[0]
            hour = dateArray[1].split(":")[0]
            print(f'  {date} - {hour}')

            if last == '0':
                last = date
                print(f'last==0')
            elif last == date:
                print(f'delete: {obj["Key"]}')
                try:
                    s3.delete_object(Bucket=bucket_name, Key='prefix' + obj["Key"])
                except ClientError as e:
                    logging.error(e)
            else:
                print(f'last = {obj["Key"]}')
                last = date
    return True


def list_bucket_objects(bucket_name):
    # Retrieve the list of bucket objects
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return None
    return response['Contents']