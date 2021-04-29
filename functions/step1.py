from .utils import MultipleOutputStep


class Step1(MultipleOutputStep):

    def get_data_iter(self, data):
        yield from range(data)

    async def do(self, data):
        self.context.logger.info(f"Step 1: received {data}")
        return await super().do(data)
