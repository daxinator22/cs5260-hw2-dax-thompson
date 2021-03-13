import boto3, json, consumer, Client, Errors, unittest, Request, S3_Widget, DynamoDB_Widget

#Before running these tests, make sure the buckets are empty:
#$ aws s3 rm s://from_bucket --recursive
class ConsumerTest(unittest.TestCase):

    def load_test_request(self):
        from_bucket = 'usu-cs5260-dax-requests'
        s3_client = boto3.client('s3')
        request = open('test-request').read()
        s3_client.put_object(Bucket=from_bucket, Key='test-request', Body=bytes(request, 'utf-8'))
        return json.loads(request)


    def test_client_method(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 'bad_method'
        to_bucket = 'usu-cs5260-dax-web'
        self.assertRaises(Errors.UnknownClientMethod, Client.Client, from_bucket, method, to_bucket)

    def test_from_bucket(self):
        from_bucket = 'bad_from_bucket'
        method = 's3'
        to_bucket = 'usu-cs5260-dax-web'
        self.load_test_request()
        client = Client.Client(from_bucket, method, to_bucket)
        self.assertRaises(Errors.BucketNotFound, client.process_next_request)

    def test_to_s3_bucket(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 's3'
        to_bucket = 'bad_to_bucket'
        client = Client.Client(from_bucket, method, to_bucket)
        self.load_test_request()
        request = client.get_request()
        self.assertRaises(Errors.BucketNotFound, client.put_widget, request.content)

    def test_to_dynamodb_bucket(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 'dynamodb'
        to_bucket = 'bad_to_bucket'
        client = Client.Client(from_bucket, method, to_bucket)
        self.load_test_request()
        request = client.get_request()
        self.assertRaises(Errors.BucketNotFound, client.put_widget, request.content)

    def test_get_request(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 's3'
        to_bucket = 'usu-cs5260-dax-web'
        client = Client.Client(from_bucket, method, to_bucket)

        correct_request = self.load_test_request()
        del correct_request['type']
        del correct_request['requestId']

        self.assertEquals(client.get_request().content, correct_request)

    def test_put_widget_s3(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 's3'
        to_bucket = 'usu-cs5260-dax-web'
        client = Client.Client(from_bucket, method, to_bucket)
        
        #Parsing tests request
        test_request = open('test-request').read()
        request = Request.Request('test-request', test_request)
        s3_widget = S3_Widget.S3_Widget(request.content)

        client.put_widget(request.content)

        #Get object from S3
        s3_client = boto3.client('s3')
        test_widget = s3_client.get_object(Bucket=to_bucket, Key=f'{s3_widget.owner}/{s3_widget.key}')['Body'].read().decode('utf-8')
        self.assertEquals(s3_widget.content, test_widget)

    def test_put_widget_dynamodb(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 'dynamodb'
        to_bucket = 'widgets'
        client = Client.Client(from_bucket, method, to_bucket)
        
        #Parsing tests request
        test_request = open('test-request').read()
        request = Request.Request('test-request', test_request)
        dynamodb_widget = DynamoDB_Widget.DynamoDB_Widget(request.content)

        client.put_widget(request.content)

        #Get object from S3
        dynamodb_client = boto3.client('dynamodb')
        test_widget = dynamodb_client.get_item(TableName=to_bucket, Key={'widgetId':{ 
                'S': request.content['widgetId']
            },
            'owner':{
                'S': request.content['owner']
            }})['Item']
        self.assertEquals(dynamodb_widget.content, test_widget)






#client_test()
unittest.main()
tests = ConsumerTest()
