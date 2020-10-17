import logging
from pipeable_worker import PipeableWorker

log = logging.getLogger('PastebinCrawler')


class Printer(PipeableWorker):
    """
    This is a basic debug worker used to print the content passed through it
    Input: Any
    Output: The input
    """

    def work(self, data):
        super().work(data)
        log.info(f'{self}: {data}')
        return data

    def handle_failed_input(self, type, value, traceback):
        log.error(f'{self}: {(type, value)}')
        return super().handle_failed_input((type, value, traceback))
