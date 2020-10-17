import sys
import abc
import queue
import logging
import threading

log = logging.getLogger('PastebinCrawler')


class RetryException(Exception):
    pass


class PipeableWorker(abc.ABC):
    POLL_TIMEOUT = 0.2
    FOLOWTHROUGH_EXCEPTIONS = (Exception,)

    def __init__(self, worker_name=None):
        self._worker_name = worker_name if \
            worker_name is not None else self.__class__.__name__
        # May be set by set_input/output_queue
        self._input_queue = None
        self._output_queue = None
        self._input_done_event = None
        self._output_done_event = None

    @abc.abstractmethod
    def work(self, data):
        """
        Perform work on a single item from the queue
        Override this method
        """
        log.debug(f'{self}: performs work on data {data}')

    def prepare(self):
        """
        Runs before working on items from the queue
        May overload this method
        """
        log.debug(f'{self}: preparing work')

    def first_pipe_prepare(self):
        """
        Runs if this worker is the first in the pipe
        May overload this method
        """
        log.debug(f'{self}: runnig as first pipe')
        # Create an dummy empty queue.
        q = queue.Queue()
        e = threading.Event()
        e.set()
        self.set_input_queue(q, e)

    def finish(self):
        """
        Runs after finished working on items from the queue.
        Will always run, even on error.
        May overload this method
        """
        log.debug(f'{self}: finished work')
        if self._output_done_event is not None:
            self._output_done_event.set()

    def __str__(self):
        return f'<{self._worker_name}>'

    def __repr__(self):
        return str(self)

    def set_input_queue(self, input_queue, input_done_event):
        self._input_queue = input_queue
        self._input_done_event = input_done_event

    def set_output_queue(self, output_queue, output_done_event):
        self._output_queue = output_queue
        self._output_done_event = output_done_event

    def input_generator(self):
        """
        Yields from the input queue if exist.
        The queue members should be a tuple of (is_success, data)
        Will only stop once input_queue is empty AND input_done_event is set.
        """
        if self._input_queue is None or self._input_done_event is None:
            # First worker in the pipe
            self.first_pipe_prepare()
        # Work as long as there is or there will be an input
        while ((not self._input_done_event.is_set()) or
               self._input_queue.unfinished_tasks):
            try:
                while True:
                    # May block up to POLL_TIMEOUT seconds
                    yield self._input_queue.get(timeout=self.POLL_TIMEOUT)
            except queue.Empty:
                # Queue is empty, check if finish all tasks
                pass

    def work_until_done(self):
        """
        Blocking function.
        Takes data from the input_generator and performs work on it.
        Will only stop once input_queue is empty AND input_done_event is set.
        """
        log.debug(f'{self}: Starting work')
        try:
            self.prepare()
        except Exception:
            log.critical(
                f'{self}: Unhandles exception while preparing', exc_info=True)
            raise
        try:
            for is_success, input_data in self.input_generator():
                is_success, output_data = self._input_handler(
                    is_success, input_data)
                # If the work returned None, no need to add it to the queue
                if output_data is not None:
                    self._add_to_out_queue(output_data, is_success=is_success)
        finally:
            self.finish()

    def _input_handler(self, is_success, input_data):
        """
        Directs input to work or handle_failed_input by is_success value.
        """
        try:
            # Handle input by working or handling errors
            if is_success:
                output_data = self.work(input_data)
            else:
                # input_data is (type, value, traceback)
                is_success, output_data = self.handle_failed_input(*input_data)
            return is_success, output_data
        except RetryException:
            self._add_to_input_queue(input_data)
            return None, None
        except self.FOLOWTHROUGH_EXCEPTIONS:
            # These exceptions will continue in the pipe
            return False, sys.exc_info()
        except Exception:
            log.error(
                f'{self}: Unhandles exception while working', exc_info=True)
            raise
        finally:
            self._input_queue.task_done()

    def _add_to_out_queue(self, output_data, is_success=True):
        if self._output_queue is not None:
            if is_success:
                log.debug(f'{self}: Sending to output: {output_data}')
            self._output_queue.put((is_success, output_data))

    def _add_to_input_queue(self, input_data, is_success=True):
        if self._input_queue is not None:
            if is_success:
                log.debug(f'{self}: Adding to input: {input_data}')
            self._input_queue.put((is_success, input_data))

    def handle_failed_input(self, type, value, traceback):
        """
        May overide this method to handle errors from previuse pipe members
        """
        return False, (type, value, traceback)
