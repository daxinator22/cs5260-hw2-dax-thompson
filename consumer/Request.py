import json

class Request():

    def __init__(self, body):
        json_object = json.loads(body)

        self.type = json_object['type']
        self.requestId = json_object['requestId']

        del json_object['type']
        del json_object['requestId']

        self.content = json_object

def get_request(client, queue):
    #Receives message from queue
    message = client.receive_message(QueueUrl=queue)['Messages'][0]
    request_object = message['Body']

    #Creates request object
    try:
        request = Request(request_object)
    except json.decoder.JSONDecodeError:
        print(message)

    #Deletes request
    client.delete_message(QueueUrl=queue, ReceiptHandle=message['ReceiptHandle'])
    
    return request


