import boto3, Request, S3_Widget, DynamoDB_Widget
from Errors import UnknownClientMethod, BucketNotFound
from botocore.exceptions import UnknownServiceError, ClientError

class Client():

    def __init__(self, from_bucket, method, to_bucket):
        self.from_bucket = from_bucket
        self.method = method
        self.to_bucket = to_bucket

        self.from_client = boto3.client('s3')
        try:
            self.to_client = boto3.client(self.method)
        except UnknownServiceError:
            raise UnknownClientMethod(f'{self.method} is not supported')

    def process_next_request(self):
        try:
            request_object = self.from_client.list_objects(Bucket=self.from_bucket, MaxKeys=1)['Contents'][0]
            request = Request.Request(request_object['Key'], self.from_client.get_object(Bucket=self.from_bucket, Key=request_object['Key'])['Body'].read().decode('utf-8'))
            if request.type == 'create':

                self.put_widget(request.content)

            self.from_client.delete_object(Bucket=self.from_bucket, Key=request.key)
        except ClientError:
            raise BucketNotFound(f'{self.from_bucket} does not exist')



    def put_widget(self, content):
        if self.method == 's3':
            try:
                widget = S3_Widget.S3_Widget(content)
                self.to_client.put_object(Bucket=self.to_bucket, Key=f'{widget.owner}/{widget.key}', Body=bytes(widget.content, 'utf-8'))
            except ClientError:
                raise BucketNotFound(f'{self.to_bucket} does not exist')

        elif self.method == 'dynamodb':
            try:
                widget = DynamoDB_Widget.DynamoDB_Widget(content)
                self.to_client.put_item(TableName=self.to_bucket, Item=widget.content)
            except:
                raise BucketNotFound(f'{self.to_bucket} does not exist')
