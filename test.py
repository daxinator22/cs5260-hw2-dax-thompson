import boto3,  json, consumer

def clear_bucket(bucket):
    resource = boto3.resource('s3')
    resource.Bucket(bucket).objects.all().delete()

def upload_request(from_bucket, method, file_name):
        clear_bucket(from_bucket)
        resource = boto3.resource(method)
        resource.Object(from_bucket, file_name).upload_file(file_name)

def compare_objects(file_name, method, to_bucket, local_file):
    resource = boto3.resource(method)
    del local_file['type']
    del local_file['requestId']
    remote_path = 'remote_file'
    resource.Object(to_bucket, local_file['widgetId']).download_file(remote_path)
    remote_file = json.loads(open(remote_path).read())

    return local_file == remote_file

#Allows the script to process one create request, and then checks the widget
def test_create_request(from_bucket, method, to_bucket, file_name):
    try:
        clear_bucket(to_bucket)
        upload_request(from_bucket, method, file_name)

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
        upload_request(from_bucket, method, file_name)

        consumer.connect(from_bucket, method, to_bucket)

        create_request = json.loads(open('create_request').read())
        local_file = json.loads(open(file_name).read())
        local_file.update(create_request)
        if compare_objects(file_name, method, to_bucket, local_file):
            print('Correct update object found')
        else:
            print('Error: incorrect update object found')


    except:
        print('Error: update failed')


def test_delete_request(from_bucket, method, to_bucket, file_name):
    try:
        upload_request(from_bucket, method, file_name)

        consumer.connect(from_bucket, method, to_bucket)

        local_file = json.loads(open(file_name).read())
        try:
            resource.Object(to_bucket, local_file['widgetId']).download_file(remote_path)
        except:
            print('Deletion completed')

    except:
        print('Error: deletion failed')


def s3_tests():
    from_bucket = 'usu-cs5260-dax-requests'
    method = 's3'
    to_bucket = 'usu-cs5260-dax-web'
    test_create_request(from_bucket, method, to_bucket, 'create_request')
    test_update_request(from_bucket, method, to_bucket, 'update_request')
    test_delete_request(from_bucket, method, to_bucket, 'delete_request')

s3_tests()

