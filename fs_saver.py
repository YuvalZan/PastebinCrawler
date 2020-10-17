import json
import logging
from pathlib import Path
from pipeable_worker import PipeableWorker

log = logging.getLogger('PastebinCrawler')

class FSSaver(PipeableWorker):
    BASE_FOLDER = Path('PasteBin')
    ERROR_LOG = BASE_FOLDER / Path('errors.log')
    SUFFIX = '.json'

    def prepare(self):
        super().prepare()
        self.BASE_FOLDER.mkdir(exist_ok=True)
        log.info(f'Writing pastes to {self.BASE_FOLDER}')

    def work(self, paste):
        super().work(paste)
        paste_path = self.BASE_FOLDER / Path(paste.id).with_suffix(self.SUFFIX)
        with open(paste_path, 'w') as paste_file:
            json.dump(paste, paste_file)

    def handle_failed_input(self, type, value, traceback):
        with open(self.ERROR_LOG, 'a') as err_log:
            err_log.write(f'{type}: {value}\n')
        return super().handle_failed_input(type, value, traceback)
