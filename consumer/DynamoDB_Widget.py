import Widget
from botocore.exceptions import ClientError

class DynamoDB_Widget(Widget.Widget):

    def convert_content(self, to_convert):
        new_content = dict()

        for attribute in to_convert:
            if attribute == 'otherAttributes':
                for extra in to_convert['otherAttributes']:
                    new_content[extra['name']] = {'S' : extra['value']}
            else:
                new_content[attribute] = {'S' : to_convert[attribute]}

        return new_content


    def __init__(self, content):
        super().__init__(content)
        self.content = self.convert_content(self.content)

    def create_widget(self, client, destination):
        client.put_item(TableName=destination, Item=self.content)

    def delete_widget(self, client, destination):
        client.delete_item(TableName=destination, Key=self.content)

    def update_widget(self, client, destination):
        try:
            client.update_item(TableName=destination, Key=self.content)
        except ClientError:
            print(f'{self.key} does not exist')
