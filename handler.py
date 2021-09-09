from abc import ABCMeta, abstractmethod
import random
import math
import uuid
import string
from rand_string.rand_string import RandString
from storey import MapClass
from storey.dtypes import Event


class _NuclioLoggerWrapper:
    def __init__(self, nuclio_logger, prefix):
        self.nuclio_logger = nuclio_logger
        self.prefix = prefix
        if not prefix.endswith(" "):
            self.prefix += " "

    def __getattr__(self, name):
        if name in ["info", "debug", "warn", "error"]:
            method = getattr(self.nuclio_logger, name)

            def wrapper(message, *args):
                return method(self.prefix + message, *args)

            return wrapper
        return getattr(self.nuclio_logger, name)


class BaseStep:
    def __init__(self, context, **kwargs):
        self.context = context
        self.logger = _NuclioLoggerWrapper(
            context.logger, f"{self.__class__.__name__} |>"
        )


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


class DataEnricher(BaseStep):
    def do(self, data):
        self.logger.info(f"Input: {data}")
        MULTIPLIER = 1000
        if random.randint(1, MULTIPLIER) > MULTIPLIER * (1 - data["err_rate"]):
            1 / 0
        data["enriched"] = math.factorial(random.randint(1, data["max_fact"]))
        self.logger.info(f"Output: {data}")
        return data


class DataFormatter(BaseStep):
    def do(self, data):
        self.logger.info(f"Input: {data}")
        data["formatted"] = "".join(
            [s for s in data["content"].lower() if s in string.ascii_lowercase]
        )[:100]
        self.logger.info(f"Output: {data}")
        return data


class ErrorCatcher(BaseStep):
    def do(self, data):
        self.logger.info(f"Input: {data}")
        return data