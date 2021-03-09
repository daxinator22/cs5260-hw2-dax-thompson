import boto3, os, json
from sys import argv

def get_object(from_bucket, object_key, file_name):
    try:
        request = boto3.resource('s3').Object(from_bucket, object_key)
        request.download_file(file_name)
        request.delete()
    except:
        print(f'Error downloading object {object_key} from S3')

def read_object(request_file, file_name):
    try:
        request_file = open(file_name)
        json_object = json.loads(request_file.read())
        request_file.close()
        return json_object
    except:
        print(f'Error reading JSON object')


def put_object(method, to_bucket, json_object, file_name):
    try:
        print(f'{json_object["requestId"]} {json_object["type"][:-1]}ing {json_object["widgetId"]} in {to_bucket} using {method}')
        to_resource = boto3.resource(method)
        if json_object['type'] == 'create':
            del json_object['type']
            del json_object['requestId']
            f = open(file_name, 'w')
            f.write(json.dumps(json_object))
            f.close()
            to_resource.Object(to_bucket, json_object['widgetId']).upload_file(file_name)
        elif json_object['type'] == 'delete':
            to_resource.Object(to_bucket, json_object['widgetId']).delete()
    except:
        print(f'Error putting JSON object')


def connect(from_bucket, method, to_bucket):
    requests = None
    try:
        from_client = boto3.client('s3')
        requests = from_client.list_objects(Bucket=from_bucket)
    except:
        print('Error retrieving objects from S3')

    file_name = 'json_file.txt'

    for request in requests['Contents']:
        get_object(from_bucket, request['Key'], file_name)
        json = read_object(file_name, file_name)
        put_object(method, to_bucket, json, file_name)
        os.remove(file_name)


if len(argv) < 4:
    print('Usage: consumer.py bucket_1 method bucket_2')
else:
    connect(argv[1], argv[2], argv[3])
