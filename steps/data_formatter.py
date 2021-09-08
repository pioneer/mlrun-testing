import string
from .base import BaseStep


class DataFormatter(BaseStep):
    def do(self, data):
        self.logger.info(f"Input: {data}")
        data["formatted"] = "".join(
            [s for s in data["content"].lower() if s in string.ascii_lowercase]
        )[:100]
        self.logger.info(f"Output: {data}")
        return data
