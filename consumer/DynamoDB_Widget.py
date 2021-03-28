import Widget

class DynamoDB_Widget(Widget.Widget):

    def convert_content(self):
        new_content = dict()
        new_content['widgetId'] = {
                'S' : self.content['widgetId']
        }

        new_content['owner'] = {
                'S' : self.content['owner']
        }

        new_content['label'] = {
                'S' : self.content['label']
        }

        new_content['description'] = {
                'S' : self.content['description']
        }

        for attribute in self.content['otherAttributes']:
            new_content[attribute['name']] = {
                    'S' : attribute['value']
                                        
            }

        self.content = new_content


    def __init__(self, content):
        super().__init__(content)
        self.convert_content()

    def create_widget(self, client, destination):
        client.put_item(TableName=destination, Item=self.content)

    def delete_widget(self, client, destintation):
        return

    def update_widget(self, client, destintation):
        return
