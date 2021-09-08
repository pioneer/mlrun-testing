from .base import BaseStep


class ErrorCatcher(BaseStep):
    def do(self, data):
        self.logger.info(f"Input: {data}")
        return data
