import logging
import time
from pipeable_worker import PipeableWorker


log = logging.getLogger('PastebinCrawler')


class Timer(PipeableWorker):
    POLL_INTERVAL = 0.2

    def __init__(self, sleep_interval, worker_name=None):
        super().__init__(worker_name=worker_name)
        self._sleep_interval = sleep_interval

    def work(self, sleep_interval):
        log.info(f'{self}: Started timer with an interval of \
            {self._sleep_interval}')
        # Run untill external shutdown
        while not self._output_done_event.is_set():
            self._add_to_out_queue(sleep_interval)
            self.sleep(sleep_interval)

    def sleep(self, seconds):
        """
        Sleeps in intervals in order to check _output_done_event
        """
        wake_time = time.time() + seconds
        while time.time() < wake_time and not self._output_done_event.is_set():
            time.sleep(self.POLL_INTERVAL)
        log.info(f'{self}: Finished sleep')

    def first_pipe_prepare(self):
        """
        If this worker is the first in the pipe than still perform one request
        """
        super().first_pipe_prepare()
        self._add_to_input_queue(self._sleep_interval)
