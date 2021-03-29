import unittest, boto3, json, time, Client, Request, DynamoDB_Widget
from botocore.exceptions import ClientError

class ConsumerUnitTests(unittest.TestCase):


    def test_get_request(self):
        queue_client = boto3.client('sqs')
        s3_client = boto3.client('s3')
        dynamodb_client = boto3.client('dynamodb')
        
        queue_url = 'https://queue.amazonaws.com/419060363036/cs5260-requests'

        #Clears queue
        try:
            queue_client.purge_queue(QueueUrl=queue_url)
        except ClientError:
            print('Sleeping for 60 seconds')
            time.sleep(60)

        #Uploads test request
        test_request = dict()
        test_request['type'] = 'create'
        test_request['requestId'] = '12345'
        test_request['widgetId'] = '12345'
        test_request['owner'] = 'Test'
        queue_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(test_request))


        #Get request
        request = Request.get_request(queue_client, queue_url)
    
        self.assertEqual(request.type, test_request['type'])
        self.assertEqual(request.requestId, test_request['requestId'])
        self.assertEqual(request.content['widgetId'], test_request['widgetId'])
        self.assertEqual(request.content['owner'], test_request['owner'])


    def test_convert_request_dynamodb(self):
        queue_url = 'https://queue.amazonaws.com/419060363036/cs5260-requests'
        method = 'dynamodb'
        destination = 'widgets'

        #Creates client
        client = Client.Client(queue_url, method, destination)

        #Creates request
        test_request = dict()
        test_request['type'] = 'create'
        test_request['requestId'] = '12345'
        test_request['widgetId'] = '12345'
        test_request['owner'] = 'Test'
        request = Request.Request(json.dumps(test_request))

        #Converts request to widget
        widget = client.convert_request_to_widget(request)

        self.assertEqual(widget.content['widgetId'], {'S' : '12345'})
        self.assertEqual(widget.content['owner'], {'S' : 'Test'})


    def test_dynamodb_dict_conversion(self):
        test_text = '{"widgetId":"12345","owner":"John Jones","description":"GEDOQ","otherAttributes":[{"name":"color","value":"pink"},{"name":"size-unit","value":"cm"}]}'
        test_dict = json.loads(test_text)

        #Creates widget
        widget = DynamoDB_Widget.DynamoDB_Widget(test_dict)

        self.assertEqual(widget.content['widgetId'], {'S' : '12345'})
        self.assertEqual(widget.content['owner'], {'S' : 'John Jones'})
        self.assertEqual(widget.content['description'], {'S' : 'GEDOQ'})
        self.assertEqual(widget.content['color'], {'S' : 'pink'})
        self.assertEqual(widget.content['size-unit'], {'S' : 'cm'})



if __name__ == '__main__':
    unittest.main()
