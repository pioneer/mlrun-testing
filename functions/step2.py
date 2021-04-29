

class Step2:

    def __init__(self, context, **kwargs):
        self.context = context

    def do(self, data):
        self.context.logger.info(f"Step 2: received {data}")
        return data.capitalize()
