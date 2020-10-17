import logging
from pipeable_worker import PipeableWorker

log = logging.getLogger('PastebinCrawler')

class Printer(PipeableWorker):

    def work(self, data):
        super().work(data)
        log.info(f'{self}: {data}')
        print(data)

    def handle_failed_input(self, type, value, traceback):
        log.error(f'{self}: {(type, value)}')
        return super().handle_failed_input((type, value, traceback))
