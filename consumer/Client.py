import boto3, Request, S3_Widget, DynamoDB_Widget, time, logging, datetime
from Errors import UnknownClientMethod, BucketNotFound
from botocore.exceptions import UnknownServiceError, ClientError

class Client():

    def __init__(self, queue, method, destination):
        logging.basicConfig(filename='log.log', level=logging.INFO)
        self.queue = queue
        self.method = method
        self.destination = destination

        self.request_type = {
            'create' : self.create_widget,
            'delete' : self.delete_widget,
            'update' : self.update_widget,
        }

        self.instant_widget = {
            's3' : S3_Widget.S3_Widget,
            'dynamodb' : DynamoDB_Widget.DynamoDB_Widget,
        }

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

        self.handle_widget(request)

    def get_request(self):
        try:
            #Gets request
            request = Request.get_request(self.queue_client, self.queue)

            #Logging info
            logging.info(f'Processing {request.type} request {request.requestId} at {datetime.datetime.now()}')
            print(f'Processing {request.type} request {request.requestId} at {datetime.datetime.now()}')

            return request

        except ClientError:
            raise BucketNotFound(f'{self.queue} does not exist')
        except KeyError:
            raise KeyError


    def handle_widget(self, request):
        try:
            #Create widget
            widget = self.instant_widget[self.method](request.content)

            #Logging info
            logging.info(f'Processing widget {widget.key} at {datetime.datetime.now()}')
            print(f'Processing widget {widget.key} at {datetime.datetime.now()}')

            #Process widget
            self.request_type[request.type](widget)
        except ClientError:
            raise BucketNotFound(f'{self.destination} does not exist')

    def create_widget(self, widget):
            widget.create_widget(self.destination_client, self.destination)

    def delete_widget(self, widget):
            widget.delete_widget(self.destination_client, self.destination)

    def update_widget(self, widget):
            widget.update_widget(self.destination_client, self.destination)
