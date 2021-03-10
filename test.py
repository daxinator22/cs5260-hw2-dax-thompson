import boto3, json, consumer

def clear_bucket(bucket):
    resource = boto3.resource('s3')
    resource.Bucket(bucket).objects.all().delete()

def upload_request(from_bucket, file_name):
    clear_bucket(from_bucket)
    resource = boto3.resource('s3')
    resource.Object(from_bucket, file_name).upload_file(file_name)

def compare_objects(file_name, method, to_bucket, local_file):
    resource = boto3.resource(method)
    del local_file['type']
    del local_file['requestId']
    remote_path = 'remote_file'
    remote_file = None
    if method == 's3':
        resource.Object(to_bucket, local_file['widgetId']).download_file(remote_path)
        remote_file = json.loads(open(remote_path).read())
    elif method == 'dynamodb':
        remote_file = resource.Table(to_bucket).get_item(Key={'widgetId': local_file['widgetId'], 'owner': local_file['owner']})['Item']

    return local_file == remote_file

#Allows the script to process one create request, and then checks the widget
def test_create_request(from_bucket, method, to_bucket, file_name):
    try:
        upload_request(from_bucket, file_name)

        consumer.connect(from_bucket, method, to_bucket)

        local_file = json.loads(open(file_name).read())
        if compare_objects(file_name, method, to_bucket, local_file):
            print('Correct creation object found')
        else:
            print('Error: incorrect creation object found')

    except:
        print('Error: creation failed')


def test_update_request(from_bucket, method, to_bucket, file_name):
    try:
        upload_request(from_bucket, file_name)

        consumer.connect(from_bucket, method, to_bucket)

        create_request = json.loads(open('create_request').read())
        local_file = json.loads(open(file_name).read())
        create_request.update(local_file)
        local_file = create_request
        if compare_objects(file_name, method, to_bucket, local_file):
            print('Correct update object found')
        else:
            print('Error: incorrect update object found')


    except:
        print('Error: update failed')


def test_delete_request(from_bucket, method, to_bucket, file_name):
    try:
        upload_request(from_bucket, file_name)
        
        consumer.connect(from_bucket, method, to_bucket)

        local_file = json.loads(open(file_name).read())
        try:
            if method == 's3':
                resource.Object(to_bucket, local_file['widgetId']).download_file(remote_path)
            elif method == 'dynamodb':
                resource.Table(to_bucket).get_item(Key={'widgetId': local_file['widgetId'], 'owner': local_file['owner']})
        except:
            print('Deletion completed')

    except:
        print('Error: deletion failed')


def s3_tests():
    from_bucket = 'usu-cs5260-dax-requests'
    method = 's3'
    to_bucket = 'usu-cs5260-dax-web'
    clear_bucket(to_bucket)
    print('Testing S3 widget creation')
    test_create_request(from_bucket, method, to_bucket, 'create_request')
    #test_update_request(from_bucket, method, to_bucket, 'update_request')
    #test_delete_request(from_bucket, method, to_bucket, 'delete_request')

def dynamodb_tests():
    from_bucket = 'usu-cs5260-dax-requests'
    method = 'dynamodb'
    to_bucket = 'widgets'
    print('Testing DynamoDB  widget creation')
    test_create_request(from_bucket, method, to_bucket, 'create_request')
    #test_update_request(from_bucket, method, to_bucket, 'update_request')
    
    


s3_tests()
print()
dynamodb_tests()
