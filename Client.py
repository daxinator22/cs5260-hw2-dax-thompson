import boto3
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
            self.from_client.list_objects(Bucket=self.from_bucket)
        except ClientError:
            raise BucketNotFound(f'{self.from_bucket} does not exist')


