import boto3, Request, S3_Widget, DynamoDB_Widget, time, logging, datetime
from Errors import UnknownClientMethod, BucketNotFound
from botocore.exceptions import UnknownServiceError, ClientError

class Client():

    def __init__(self, from_bucket, method, to_bucket):
        logging.basicConfig(filename='log.log', level=logging.INFO)
        self.from_bucket = from_bucket
        self.method = method
        self.to_bucket = to_bucket

        self.from_client = boto3.client('sqs')
        try:
            self.to_client = boto3.client(self.method)
        except UnknownServiceError:
            raise UnknownClientMethod(f'{self.method} is not supported')

    def process_next_request(self):
        request = None
        try:
            request = self.get_request()
        except KeyError:
            time.sleep(0.1)
            return

        if request.type == 'create':
            self.put_widget(request.content)

    def get_request(self):
        try:
            #Receives message from queue
            message = self.from_client.receive_message(QueueUrl=self.from_bucket)['Messages'][0]
            request_object = message['Body']

            #Creates request object
            request = Request.Request(request_object)
            
            #Deletes request
            self.from_client.delete_message(QueueUrl=self.from_bucket, ReceiptHandle=message['ReceiptHandle'])

            #Logging info
            logging.info(f'Processing create request {request.requestId} at {datetime.datetime.now()}')
            print(f'Processing {request.type} request {request.requestId} at {datetime.datetime.now()}')

            return request

        except ClientError:
            raise BucketNotFound(f'{self.from_bucket} does not exist')
        except KeyError:
            raise KeyError


    def put_widget(self, content):
        if self.method == 's3':
            try:
                widget = S3_Widget.S3_Widget(content)
                logging.info(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                print(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                self.to_client.put_object(Bucket=self.to_bucket, Key=f'{widget.owner}/{widget.key}', Body=bytes(widget.content, 'utf-8'))
            except ClientError:
                raise BucketNotFound(f'{self.to_bucket} does not exist')

        elif self.method == 'dynamodb':
            try:
                widget = DynamoDB_Widget.DynamoDB_Widget(content)
                logging.info(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                print(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                self.to_client.put_item(TableName=self.to_bucket, Item=widget.content)
            except:
                raise BucketNotFound(f'{self.to_bucket} does not exist')
