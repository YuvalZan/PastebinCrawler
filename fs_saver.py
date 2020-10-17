import json
import logging
import glob
import threading
from pathlib import Path
from pipeable_worker import PipeableWorker
from cacher import Cacher

log = logging.getLogger('PastebinCrawler')
BASE_FOLDER = Path('.cache')
SUFFIX = '.json'


class FSSaver(PipeableWorker):
    """
    This worker saves each input paste to a json file with its id as a name.
    It will also save each exception from the input to a log file.
    Input: Paste objects
    Output: The input

    """
    ERROR_LOG = BASE_FOLDER / Path('errors.log')
    LOG_LOCK = threading.Lock()

    def prepare(self):
        super().prepare()
        BASE_FOLDER.mkdir(exist_ok=True)

    def work(self, paste):
        super().work(paste)
        paste_path = BASE_FOLDER / Path(paste.id).with_suffix(SUFFIX)
        log.info(f'{self}: Saving paste {paste.id} to {paste_path}')
        with open(paste_path, 'w') as paste_file:
            json.dump(paste, paste_file)
        return paste

    def handle_failed_input(self, type, value, traceback):
        with self.LOG_LOCK:
            with open(self.ERROR_LOG, 'a') as err_log:
                err_log.write(f'{type}: {value}\n')
        return super().handle_failed_input(type, value, traceback)


class FSCacher(Cacher):
    """
    This worker is similar to the Cacher worker in its normal behavior.
    The difference is that it will initialize the cache by checking the local
    FS for any saved pastes.
    Input: Any hashable
    Output: The input
    Notice: The input must be hashable or a TypeError will be raised
    """

    def prepare(self):
        super().prepare()
        log.info(f'{self}: Adding previuse paste from disk to cache')
        for file_path in \
                glob.glob(str(BASE_FOLDER / Path('*').with_suffix(SUFFIX))):
            paste_id = Path(file_path).stem
            self.add_to_cache(paste_id)
