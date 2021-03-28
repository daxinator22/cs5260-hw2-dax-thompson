import Widget, json

class S3_Widget(Widget.Widget):

    def __init__(self, content):
        super().__init__(content)
        self.content_to_string()

    def content_to_string(self):
        self.content = json.dumps(self.content)

    def create_widget(self, client, destination):
        client.put_object(Bucket=destination, Key=f'{self.owner}/{self.key}', Body=bytes(self.content, 'utf-8'))

    def delete_wiget(self, client, destination):
        client.delete_object(Bucket=destination, Key=self.key)

    def update_widget(self, client, destination):
        old_widget = None
        try:

            #Gets old object from S3
            old_object = client.get_object(Bucket=destination, Key=f'{self.owner}/{self.key}')['Body'].read()
            old_widget = json.loads(old_object)
            
            #Updates object
            old_widget.update(self.content)

            #Creates new widget
            self.content = old_widget
            self.create_widget(client, destination)

        except client.exceptions.NoSuchKey:
            print(f'Could not find key {self.key}')

        except ValueError as e:
            print(old_widget)
            print(e)
