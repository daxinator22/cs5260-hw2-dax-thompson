import boto3, os, json
from sys import argv

def connect(from_bucket, method, to_bucket):
    try:
        from_client = boto3.client('s3')
        requests = from_client.list_objects(Bucket=from_bucket)
        for request in requests['Contents']:
            request = boto3.resource('s3').Object(from_bucket, request['Key'])
            request.download_file('file')
            request_file = open('file')
            json_object = json.loads(request_file.read())
            request_file.close()
            to_resource = boto3.resource(method)
            to_resource.Object(to_bucket, json_object['widgetId']).upload_file('file')
            os.remove('file')

    except:
        print('Unable to connect')

if len(argv) < 4:
    print('Usage: consumer.py bucket_1 method bucket_2')
else:
    connect(argv[1], argv[2], argv[3])
