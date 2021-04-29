from mlrun import get_dataitem
from .utils import MultipleOutputStep


class Step1(MultipleOutputStep):

    def get_data_iter(self, data):
        for line in self.file.get().splitlines():
            yield line

    async def do(self, data):
        self.context.logger.info(f"Step 1: received {data}")
        self.file = get_dataitem(f"./artifacts/{data}")
        self.context.logger.info(f"Step 1: file {self.file}")
        return await super().do(data)
