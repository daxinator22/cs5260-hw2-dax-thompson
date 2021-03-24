import datetime, boto3

class Widget():

    def __init__(self, content):

        self.key = content['widgetId']
        self.owner = content['owner']
        self.content = content

    def create_widget(self, client, destination):
        return


