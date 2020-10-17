import logging
from pipe_manager import PipeManager
from pastebin_workers import InitPastebinWorker, SinglePastebinWorker
from printer_worker import Printer
from fs_saver import FSSaver, FSCacher
from timer import Timer

MAX_REQUEST_WORKERS = 8
MAX_SAVE_WORKERS = 2
LOG_MAX_SIZE_BYTES = 1024 * 1024
LOG_MAX_BACKUPS = 2
TIMER_INTERVAL = 60 * 2
LOG_LEVEL = logging.INFO
# LOG_LEVEL = logging.DEBUG

log = logging.getLogger('PastebinCrawler')

def init_logger(level, log_path=None):
    log.setLevel(level)
    formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formater)
    log.addHandler(stream_handler)
    if log_path:
        file_handler = logging.RotatingFileHandler(log_path, maxBytes=LOG_MAX_SIZE_BYTES, backupCount=LOG_MAX_BACKUPS)
        file_handler.setFormatter(formater)
        file_handler.setLevel(logging.DEBUG)
        log.addHandler(file_handler)

def main():
    init_logger(LOG_LEVEL)
    pastebin_squad = [SinglePastebinWorker(f'SinglePastebin_{i}') for i in range(MAX_REQUEST_WORKERS)]
    fs_saver_squad = [FSSaver(f'FSSaver_{i}') for i in range(MAX_SAVE_WORKERS)]
    timer = Timer(TIMER_INTERVAL)
    manager = PipeManager([[timer], [InitPastebinWorker()], [FSCacher()], pastebin_squad, fs_saver_squad, [Printer()]])
    manager.run()

if __name__ == '__main__':
    main()