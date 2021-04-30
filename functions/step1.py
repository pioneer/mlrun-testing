from threading import local
from mlrun import get_dataitem
from .utils import MultipleOutputStep


class Step1(MultipleOutputStep):

    def get_data_iter(self, data):
        self.context.logger.info(f"Step 1[get_data_iter]: {data}, {type(data)}")
        for line in self.file.get().splitlines():
            yield line

    async def do(self, data):
        self.context.logger.info(f"Step 1: received {data}")
        self.file = get_dataitem(f"./artifacts/{data}")
        local_path = self.file.local()
        self.context.logger.info(f"Step 1: file {local_path}")
        return await super().do(data)


class Step1Gen:

    def __init__(self, context, **kwargs):
        self.context = context

    def get_data_iter(self, data):
        self.context.logger.info(f"Step 1[get_data_iter]: {data}, {type(data)}")
        for line in self.file.get().splitlines():
            yield line

    def do(self, data):
        self.context.logger.info(f"Step 1: received {data}")
        self.file = get_dataitem(f"./artifacts/{data}")
        local_path = self.file.local()
        self.context.logger.info(f"Step 1: file {local_path}")
        return self.get_data_iter(data)