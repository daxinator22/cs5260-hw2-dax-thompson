import unittest, boto3, json, time, Client, Request, DynamoDB_Widget, S3_Widget
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


    def test_convert_request_s3(self):
        queue_url = 'https://queue.amazonaws.com/419060363036/cs5260-requests'
        method = 's3'
        destination = 'usu-cs5260-dax-web'

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

        self.assertEqual(widget.content, '{"widgetId": "12345", "owner": "Test"}')


    def test_update_widget_s3(self):
        queue_url = 'https://queue.amazonaws.com/419060363036/cs5260-requests'
        method = 's3'
        destination = 'usu-cs5260-dax-web'

        s3_client = boto3.client('s3')

        #Creates client
        client = Client.Client(queue_url, method, destination)

        #Creates sample object
        test_dict = dict()
        test_dict['widgetId'] = '12345'
        test_dict['owner'] = 'test'
        test_dict['label'] = 'another test'

        #Puts object in bucket
        s3_client.put_object(Bucket=destination, Body=bytes(json.dumps(test_dict), 'utf-8'), Key=f'{test_dict["owner"]}/{test_dict["widgetId"]}')

        #Changes dictionary
        del test_dict['label']
        test_dict['description'] = 'yet another test'

        #Updates object in S3
        widget = S3_Widget.S3_Widget(test_dict)
        widget.update_widget(s3_client, destination)

        #Gets updated object from S3
        return_object = s3_client.get_object(Bucket=destination, Key=f'{test_dict["owner"]}/{test_dict["widgetId"]}')
        updated_object = json.loads(return_object['Body'].read())

        #Compares the dictionaries
        test_dict['label'] = 'another test'
        self.assertEqual(updated_object, test_dict)

        #Deletes test object in S3
        s3_client.delete_object(Bucket=destination, Key=f'{test_dict["owner"]}/{test_dict["widgetId"]}')
        

    def test_update_widget_dynamodb(self):
        queue_url = 'https://queue.amazonaws.com/419060363036/cs5260-requests'
        method = 'dynamodb'
        destination = 'widgets'

        dynamodb_client = boto3.client('dynamodb')

        #Creates client
        client = Client.Client(queue_url, method, destination)

        #Creates sample object
        db_dict = dict()
        db_dict['widgetId'] = {'S' : '12345'}
        db_dict['owner'] = {'S' : 'test'}
        db_dict['label'] = {'S' : 'another test'}
        
        #Puts object in DynamoDB table
        dynamodb_client.put_item(TableName=destination, Item=db_dict)

        #Updates object in S3
        test_dict = dict()
        test_dict['widgetId'] = '12345'
        test_dict['owner'] = 'test'
        test_dict['description'] = 'yet another test'
        widget = DynamoDB_Widget.DynamoDB_Widget(test_dict)
        widget.update_widget(dynamodb_client, destination)

        #Gets updated object from DynamoDB
        return_item = dynamodb_client.get_item(TableName=destination, Key={'widgetId' : db_dict['widgetId'], 'owner' : db_dict['owner']})
        db_dict['description'] = {'S' : 'yet another test'}

        self.assertEqual(db_dict, return_item['Item'])

        #Deletes item from DynamoDB
        dynamodb_client.delete_item(TableName=destination, Key={'widgetId' : db_dict['widgetId'], 'owner' : db_dict['owner']})



if __name__ == '__main__':
    unittest.main()
