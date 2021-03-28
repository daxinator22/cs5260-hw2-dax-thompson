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
            #Gets request
            request = Request.get_request(self.queue_client, self.queue)

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
                #Create widget
                widget = S3_Widget.S3_Widget(content)

                #Logging info
                logging.info(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                print(f'Processing widget {widget.key} at {datetime.datetime.now()}')

                #Process widget
                widget.create_widget(self.destination_client, self.destination)
            except ClientError:
                raise BucketNotFound(f'{self.destination} does not exist')

        elif self.method == 'dynamodb':
            try:
                #Create widget
                widget = DynamoDB_Widget.DynamoDB_Widget(content)

                #Logging info
                logging.info(f'Processing widget {widget.key} at {datetime.datetime.now()}')
                print(f'Processing widget {widget.key} at {datetime.datetime.now()}')

                #Process widget
                widget.create_widget(self.destination_client, self.destination)
            except:
                raise BucketNotFound(f'{self.destination} does not exist')
