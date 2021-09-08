import random
import math
from .base import BaseStep


class DataEnricher(BaseStep):
    def do(self, data):
        self.logger.info(f"Input: {data}")
        MULTIPLIER = 1000
        if random.randint(1, MULTIPLIER) > MULTIPLIER * (1 - data["err_rate"]):
            1 / 0
        data["enriched"] = math.factorial(random.randint(1, data["max_fact"]))
        self.logger.info(f"Output: {data}")
        return data
