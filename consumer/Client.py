import boto3, Request, S3_Widget, DynamoDB_Widget, time, logging, datetime
from Errors import UnknownClientMethod, BucketNotFound
from botocore.exceptions import UnknownServiceError, ClientError

class Client():

    def __init__(self, queue, method, destination):
        logging.basicConfig(filename='log.log', level=logging.INFO)
        self.queue = queue
        self.method = method
        self.destination = destination

        self.queue_client = boto3.client('sqs')
        try:
            self.destination_client = boto3.client(self.method)
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
            message = self.queue_client.receive_message(QueueUrl=self.queue)['Messages'][0]
            request_object = message['Body']

            #Creates request object
            request = Request.Request(request_object)
            
            #Deletes request
            self.queue_client.delete_message(QueueUrl=self.queue, ReceiptHandle=message['ReceiptHandle'])

            #Logging info
            logging.info(f'Processing create request {request.requestId} at {datetime.datetime.now()}')
            print(f'Processing {request.type} request {request.requestId} at {datetime.datetime.now()}')

            return request

        except ClientError:
            raise BucketNotFound(f'{self.queue} does not exist')
        except KeyError:
            raise KeyError


    def put_widget(self, content):
        if self.method == 's3':
            try:
                widget = S3_Widget.S3_Widget(content)
                logging.info(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                print(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                self.destination_client.put_object(Bucket=self.destination, Key=f'{widget.owner}/{widget.key}', Body=bytes(widget.content, 'utf-8'))
            except ClientError:
                raise BucketNotFound(f'{self.destination} does not exist')

        elif self.method == 'dynamodb':
            try:
                widget = DynamoDB_Widget.DynamoDB_Widget(content)
                logging.info(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                print(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                self.destination_client.put_item(TableName=self.destination, Item=widget.content)
            except:
                raise BucketNotFound(f'{self.destintation} does not exist')
