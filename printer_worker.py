import logging
from pipeable_worker import PipeableWorker

log = logging.getLogger('PastebinCrawler')

class Printer(PipeableWorker):
    DEFAULT_WORKER_NAME = 'Printer'

    def work(self, data):
        log.info(f'{self}: {data}')
        print(data)

    def handle_failed_input(self, type, value, traceback):
        log.error(f'{self}: {(type, value)}')
        return super().handle_failed_input((type, value, traceback))
