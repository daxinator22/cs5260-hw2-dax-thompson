import Widget, json

class S3_Widget(Widget.Widget):

    def __init__(self, content):
        super().__init__(content)
        self.content_to_string()

    def create_widget(self, client, destination):
        client.put_object(Bucket=destination, Key=f'{self.owner}/{self.key}', Body=bytes(self.content, 'utf-8'))


    def content_to_string(self):
        self.content = json.dumps(self.content)
