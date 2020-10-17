import logging
from pipeable_worker import PipeableWorker

log = logging.getLogger('PastebinCrawler')

class Cacher(PipeableWorker):
    # None can only be added artificially to the pipe
    DISABLE_CACHE_VALUES = [None]

    def __init__(self, worker_name=None):
        super().__init__(worker_name=worker_name)
        self._cache = set()

    def work(self, data):
        super().work(data)
        if data in self.DISABLE_CACHE_VALUES:
            return data
        if data not in self._cache:
            self.add_to_cache(data)
            return data
        log.debug(f'{self}: Dropped cached work: {data}')

    def add_to_cache(self, data):
        log.debug(f"{self}: Adding {data} to cache")
        self._cache.add(data)

    def remove_from_cache(self, data):
        log.debug(f"{self}: Removing {data} from cache")
        self._cache.remove(data)
