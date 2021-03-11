import boto3, Request, Widget, json
from Errors import UnknownClientMethod, BucketNotFound
from botocore.exceptions import UnknownServiceError, ClientError
from botocore.errorfactory import ResourceNotFoundException

class Client():

    def __init__(self, from_bucket, method, to_bucket):
        self.from_bucket = from_bucket
        self.method = method
        self.to_bucket = to_bucket

        self.from_client = boto3.client('s3')
        try:
            if method == 's3':
                self.to_client = boto3.client(self.method)
            else:
                self.to_client = boto3.resource(self.method)

        except UnknownServiceError:
            raise UnknownClientMethod(f'{self.method} is not supported')

    def process_next_request(self):
        try:
            request_object = self.from_client.list_objects(Bucket=self.from_bucket, MaxKeys=1)['Contents'][0]
            request = Request.Request(request_object['Key'], self.from_client.get_object(Bucket=self.from_bucket, Key=request_object['Key'])['Body'].read().decode('utf-8'))
            if request.type == 'create':
                widget = Widget.Widget(request.content)
                self.put_widget(widget)

            self.from_client.delete_object(Bucket=self.from_bucket, Key=request.key)
        except ClientError:
            raise BucketNotFound(f'{self.from_bucket} does not exist')



    def put_widget(self, widget):
        if self.method == 's3':
            try:
                self.to_client.put_object(Bucket=self.to_bucket, Key=f'{widget.owner}/{widget.key}', Body=bytes(json.dumps(widget.content), 'utf-8'))
            except ClientError:
                raise BucketNotFound(f'{self.to_bucket} does not exist')

        elif self.method == 'dynamodb':
            try:
                self.to_client.Table(self.to_bucket).put_item(Item=widget.content)
            except ResourceNotFoundException:
                raise BucketNotFound(f'{self.to_bucket} does not exist')
