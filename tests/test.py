import boto3,  json
from . import consumer

def clear_bucket(bucket):
    resource = boto3.resource('s3')
    resource.Bucket(bucket).objects.all().delete()

#Allows the script to process one create request, and then checks the widget
def test_create_request(from_bucket, method, to_bucket, file_name):
    try:
        clear_bucket(from_bucket)
        clear_bucket(to_bucket)
        resource = boto3.resource(method)
        resource.Object(from_bucket, file_name).upload_file(file_name)

        consumer.connect(from_bucket, method, to_bucket)

        local_file = json.loads(open(file_name).read())
        remote_path = 'remote_file'
        resource.Object(to_bucket, local_file['widgetId']).download_file(remote_path)
        remote_file = json.loads(open(remote_path).read())
        print(remote_file)
    except:
        print('Error: creation failed')



def s3_tests():
    from_bucket = 'usu-cs5260-dax-requests'
    method = 's3'
    to_bucket = 'usu-cs5260-dax-web'
    test_create_request(from_bucket, method, to_bucket, 'create_request')

s3_tests()

