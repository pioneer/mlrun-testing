import uuid
from rand_string.rand_string import RandString
from .base import BaseStep


class DataGenerator(BaseStep):
    def get_data_iter(self, data):
        self.logger.info(f"get_data_iter: {data}, {type(data)}")
        chunk_size = data.get("chunk_size", 1024)
        num_events = data.get("num_events", 1000)
        max_fact = data.get("max_fact", 1000)
        err_rate = data.get("err_rate", 0.9)
        for i in range(num_events):
            item = {
                "run_id": data["run_id"],
                "event_num": i,
                "content": RandString("ascii", chunk_size),
                "max_fact": max_fact,
                "err_rate": err_rate,
            }
            self.logger.info(f"Output: {item}")
            yield item

    async def do(self, data):
        self.logger.info(f"Input: {data}")
        data["run_id"] = str(uuid.uuid4())
        return self.get_data_iter(data)
