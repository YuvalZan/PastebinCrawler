import logging
from pipe_manager import PipeManager
from pastebin_workers import InitPastebinWorker, SinglePastebinWorker
from printer_worker import Printer

MAX_WORKERS = 8

log = logging.getLogger('PastebinCrawler')

def init_logger():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # log.setLevel(logging.DEBUG)

def main():
    init_logger()
    init_worker = InitPastebinWorker()
    pastebin_squad = [SinglePastebinWorker(f'SinglePastebin_{i}') for i in range(MAX_WORKERS)]
    printer = Printer()
    manager = PipeManager([[init_worker], pastebin_squad, [printer]])
    manager.run()

if __name__ == '__main__':
    main()