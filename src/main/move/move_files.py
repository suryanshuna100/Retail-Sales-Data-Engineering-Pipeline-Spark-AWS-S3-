import traceback
from src.main.utility.logging_config import *

def move_s3_to_s3(s3_client,bucket_name, source_prefix,destination_prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        ''' how response looks like when we list the files in s3 bucket
        {
        'Name': 'my-bucket',
        'Prefix': 'data/sales/',
        'KeyCount': 3,
        'MaxKeys': 1000,
        'IsTruncated': False,
        'Contents': [
            {'Key': 'data/sales/',        'Size': 0,    'LastModified': ...},
            {'Key': 'data/sales/jan.csv', 'Size': 2048, 'LastModified': ...},
            {'Key': 'data/sales/feb.csv', 'Size': 1890, 'LastModified': ...},
        ]
        }
'''     

        for obj in response.get('Contents', []):
            source_key = obj['Key']
            destination_key = destination_prefix + source_key[len(source_prefix):]
            # it looks like -> destination_key = data/error_files +jan.csv --> data/error_files/jan.csv

            s3_client.copy_object(Bucket=bucket_name,
                                  CopySource={'Bucket': bucket_name,
                                              'Key': source_key}, Key=destination_key)

            s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        return f"Data Moved succesfully from {source_prefix} to {destination_prefix}"
    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e
