import boto3, os, json, logging
from sys import argv

def get_object(from_bucket, object_key, file_name):
    try:
        request = boto3.resource('s3').Object(from_bucket, object_key)
        request.download_file(file_name)
        #request.delete()
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

def write_json_object(file_name, json_object):
    f = open(file_name, 'w')
    f.write(json.dumps(json_object))
    f.close()

def put_object_s3(method, to_bucket, json_object, file_name):

    try:
        #print(f'{json_object["requestId"]} {json_object["type"][:-1]}ing {json_object["widgetId"]} in {to_bucket} using {method}')
        to_resource = boto3.resource(method)
        if json_object['type'] == 'create':
            del json_object['type']
            del json_object['requestId']
            write_json_object(file_name, json_object)
            to_resource.Object(to_bucket, json_object['widgetId']).upload_file(file_name)
            logging.info(f'Creating {json_object["widgetId"]} in {to_bucket} using {method}')
            '''        elif json_object['type'] == 'delete':
            to_resource.Object(to_bucket, json_object['widgetId']).delete()
        elif json_object['type'] == 'update':
            del json_object['type']
            del json_object['requestId']
            to_resource.Object(to_bucket, json_object['widgetId']).download_file('get_object.txt')
            update_object = json.loads(open('get_object.txt').read())
            json_object.update(update_object)
            write_json_object(file_name, json_object)
            to_resource.Object(to_bucket, json_object['widgetId']).upload_file(file_name)
'''
    except:
        logging.warning(f'Unable to create {json_object["widgetId"]} in {to_bucket} using {method}')
        

def put_object_dynamo(method, to_bucket, json_object, file_name):
    resource = boto3.resource(method)
    table = resource.Table(to_bucket)
    request_type = json_object['type']
    del json_object['type']
    del json_object['requestId']
    if request_type == 'create':
        table.put_item(Item=json_object)
        logging.info(f'Creating {json_object["widgetId"]} in {to_bucket} using {method}')
        ''' elif request_type == 'update':
        old_item = table.get_item(Key={'widgetId': json_object['widgetId'], 'owner': json_object['owner']})['Item']
        old_item.update(json_object)
        table.put_item(Item=old_item)
'''

def put_object(method, to_bucket, json, file_name):
    if method == 's3':
        put_object_s3(method,to_bucket, json, file_name)
    elif method == 'dynamodb':
        put_object_dynamo(method,to_bucket, json, file_name)
    else:
        print(f'Unknown method {method}')


def connect(from_bucket, method, to_bucket):
    logging.basicConfig(filename='log.log', level=logging.INFO)
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
