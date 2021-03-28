
class Widget():

    def __init__(self, content):

        self.key = content['widgetId']
        self.owner = content['owner']
        self.content = content

    def create_widget(self, client, destination):
        print('The create_widget method must be applied in every child class')

    def delete_widget(self, client, destination):
        print('The delete_widget method must be applied in every child class')

    def update_widget(self, client, destination):
        print('The update_widget method must be applied in every child class')

