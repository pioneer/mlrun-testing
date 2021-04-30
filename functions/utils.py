from abc import ABCMeta, abstractmethod
from storey import MapClass
from storey.dtypes import Event


class MultipleOutputStep(MapClass, metaclass=ABCMeta):

    @abstractmethod
    def get_data_iter(self, data):
        yield data

    async def do(self, data):
        data_iter = self.get_data_iter(data)
        buffer_item = next(data_iter)
        for item in data_iter:
            event = Event(body=buffer_item)
            await self._do_downstream(event)
            buffer_item = item
        return buffer_item  # we have to return a value here, hence the split
