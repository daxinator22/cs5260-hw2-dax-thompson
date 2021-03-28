import Widget, json
from dict_recursive_update import recursive_update

class S3_Widget(Widget.Widget):

    def __init__(self, content):
        super().__init__(content)
        self.content_to_string()

    def content_to_string(self):
        self.content = json.dumps(self.content)

    def create_widget(self, client, destination):
        client.put_object(Bucket=destination, Key=f'{self.owner}/{self.key}', Body=bytes(self.content, 'utf-8'))

    def delete_widget(self, client, destination):
        client.delete_object(Bucket=destination, Key=self.key)

    def update_widget(self, client, destination):
        old_widget = None
        try:

            #Gets old object from S3
            old_object = client.get_object(Bucket=destination, Key=f'{self.owner}/{self.key}')['Body'].read()
            old_widget = json.loads(old_object)
            
            #Updates object
            recursive_update(old_widget, json.loads(self.content))

            #Creates new widget
            self.content = json.dumps(old_widget)
            self.create_widget(client, destination)

        except client.exceptions.NoSuchKey:
            print(f'Could not find key {self.key}')

        except ValueError as e:
            print(f'Old Widget: {old_widget}\nNew Widget: {self.content}')
