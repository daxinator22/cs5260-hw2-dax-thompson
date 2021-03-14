import json

class Request():

    def __init__(self, key, body):
        json_object = json.loads(body)

        self.key = key
        self.type = json_object['type']
        self.requestId = json_object['requestId']

        del json_object['type']
        del json_object['requestId']

        self.content = json_object
