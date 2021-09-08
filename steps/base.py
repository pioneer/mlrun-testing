from abc import ABCMeta, abstractmethod
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
