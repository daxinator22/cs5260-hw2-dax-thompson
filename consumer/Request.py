import json

class Request():

    def __init__(self, body):
        json_object = json.loads(body)

        self.type = json_object['type']
        self.requestId = json_object['requestId']

        del json_object['type']
        del json_object['requestId']

        self.content = json_object
