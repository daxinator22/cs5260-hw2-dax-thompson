import Widget, json

class S3_Widget(Widget.Widget):

    def __init__(self, content):
        super().__init__(content)
        self.content_to_string()

    def content_to_string(self):
        self.content = json.dumps(self.content)
