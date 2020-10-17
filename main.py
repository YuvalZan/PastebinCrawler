import logging
from pipe_manager import PipeManager
from pastebin_workers import InitPastebinWorker, SinglePastebinWorker
from printer_worker import Printer
from fs_saver import FSSaver

MAX_WORKERS = 16

log = logging.getLogger('PastebinCrawler')

def init_logger():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # log.setLevel(logging.DEBUG)

def main():
    init_logger()
    init_worker = InitPastebinWorker()
    pastebin_squad = [SinglePastebinWorker(f'SinglePastebin_{i}') for i in range(MAX_WORKERS)]
    printer = Printer()
    manager = PipeManager([[init_worker], pastebin_squad, [FSSaver()], [printer]])
    manager.run()

if __name__ == '__main__':
    main()