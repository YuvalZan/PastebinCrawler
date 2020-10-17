import logging
from pipe_manager import PipeManager
from pastebin_workers import InitPastebinWorker, SinglePastebinWorker
from printer_worker import Printer
from fs_saver import FSSaver, FSCacher

MAX_WORKERS = 8

log = logging.getLogger('PastebinCrawler')

def init_logger():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # log.setLevel(logging.DEBUG)

def main():
    init_logger()
    pastebin_squad = [SinglePastebinWorker(f'SinglePastebin_{i}') for i in range(MAX_WORKERS)]
    manager = PipeManager([[InitPastebinWorker()], [FSCacher()], pastebin_squad, [FSSaver()]])
    manager.run()

if __name__ == '__main__':
    main()