import boto3, Request, S3_Widget, DynamoDB_Widget, time, logging, datetime
from Errors import UnknownClientMethod, BucketNotFound
from botocore.exceptions import UnknownServiceError, ClientError

class Client():

    def __init__(self, queue, method, destination):
        logging.basicConfig(filename='log.log', level=logging.INFO)
        self.queue = queue
        self.method = method
        self.destination = destination

        self.types = {
            'create' : self.create_widget,
            'update' : self.update_widget,
            'delete' : self.delete_widget,
        }

        self.widget_types = {
            's3': S3_Widget.S3_Widget,
            'dynamodb': DynamoDB_Widget.DynamoDB_Widget,
        }

        self.queue_client = boto3.client('s3')
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

        self.types[request.type](request.content)

    def get_request(self):
        try:
            request_object = self.queue_client.list_objects(Bucket=self.queue, MaxKeys=1)['Contents'][0]
            request = Request.Request(request_object['Key'], self.queue_client.get_object(Bucket=self.queue, Key=request_object['Key'])['Body'].read().decode('utf-8'))
            logging.info(f'Processing create request {request.key} at {datetime.datetime.now()}')
            print(f'Processing {request.type} request {request.key} at {datetime.datetime.now()}')
            self.queue_client.delete_object(Bucket=self.queue, Key=request.key)
            return request
        except ClientError:
            raise BucketNotFound(f'{self.queue} does not exist')
        except KeyError:
            raise KeyError

    def update_widget(self, content):
        #TODO: Update Widget
        return

    def delete_widget(self, content):
        #TODO: Delete Widget
        return


    def create_widget(self, content):
        #Creates Widget instance based on method
        widget = self.widget_types[self.method](content)

        try:
            logging.info(f'Processing widget {widget.key} at {datetime.datetime.now()}')
            print(f'Processing widget {widget.key} at {datetime.datetime.now()}')

            #Calls parent create_widget method
            widget.create_widget(self.to_client, self.destination)
        except ClientError:
            raise BucketNotFound(f'{self.destination} does not exist')

