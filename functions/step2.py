

class Step2:

    def __init__(self, context, **kwargs):
        self.context = context

    def do(self, number):
        self.context.logger.info(f"Step 2: received {number}")
        return number*10
