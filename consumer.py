import boto3, os
from sys import argv

def connect(from_bucket, method, to_bucket):
    try:
        client = boto3.client('s3')
        requests = client.list_objects(Bucket=from_bucket)
        request = boto3.resource('s3').Object(from_bucket, requests['Contents'][0]['Key'])
        request.download_file('file')
        request_file = open('file')
        print(request_file.read())
        request_file.close()
        os.remove('file')

    except:
        print('Unable to connect')

if len(argv) < 4:
    print('Usage: consumer.py bucket_1 method bucket_2')
else:
    connect(argv[1], argv[2], argv[3])
