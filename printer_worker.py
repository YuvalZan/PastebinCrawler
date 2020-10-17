import logging
from pipeable_worker import PipeableWorker

log = logging.getLogger('PastebinCrawler')

class Printer(PipeableWorker):
    DEFAULT_WORKER_NAME = 'Printer'

    def work(self, data):
        log.info(f'{self}: {data}')
        print(data)

    def _handle_failed_input(self, err_data):
        log.error(f'{self}: {err_data}')
        return super()._handle_failed_input(err_data)
