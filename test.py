import boto3, json, consumer, Client, Errors, unittest, Request


class ConsumerTest(unittest.TestCase):
    def test_client_method(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 'bad_method'
        to_bucket = 'usu-cs5260-dax-web'
        self.assertRaises(Errors.UnknownClientMethod, Client.Client, from_bucket, method, to_bucket)

    def test_from_bucket(self):
        from_bucket = 'bad_from_bucket'
        method = 's3'
        to_bucket = 'usu-cs5260-dax-web'
        client = Client.Client(from_bucket, method, to_bucket)
        self.assertRaises(Errors.BucketNotFound, client.process_next_request)

    def test_to_s3_bucket(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 's3'
        to_bucket = 'bad_to_bucket'
        client = Client.Client(from_bucket, method, to_bucket)
        self.assertRaises(Errors.BucketNotFound, client.process_next_request)

    def test_to_dynamodb_bucket(self):
        from_bucket = 'usu-cs5260-dax-requests'
        method = 'dynamodb'
        to_bucket = 'bad_to_bucket'
        client = Client.Client(from_bucket, method, to_bucket)
        self.assertRaises(Errors.BucketNotFound, client.process_next_request)

    def test_request_processing(self):
        client = boto3.client('s3')
        request = open('test-request').read()
        correct_request = json.loads(request)
        del correct_request['type']
        del correct_request['requestId']
        self.assertEquals(Request.Request('test', request).content, correct_request)





def request_test():
    test = unittest.TestCase()
    from_bucket = 'usu-cs5260-dax-requests'
    method = 's3'
    to_bucket = 'usu-cs5260-dax-web'
    test_request = json.loads(open('test-request').read())
    request = Request.Request('test-request', test_request)





#client_test()
unittest.main()
tests = ConsumerTest()
