import logging
import queue
import threading
from concurrent import futures

log = logging.getLogger('PastebinCrawler')

class PipeManager():
    MAX_WORKERS = 16

    def __init__(self, pipable_squad_list, queue_maxsize=0):
        """
        Initiates a pipe of workers.
        :param pipable_squad_list: A list of squads where each squad is a list of workers
        """
        self._pipable_squad_list = pipable_squad_list
        self._queue_maxsize = queue_maxsize
        self._queues = None
        self._events = None

    def _init_pipe(self):
        """
        Connect all squads in pipe
        """
        connections_count = len(self._pipable_squad_list) - 1
        if connections_count <= 0:
            # No connection to setup
            return
        # Creates a queue for every connection between two squads
        self._queues = [queue.Queue(self._queue_maxsize) for _ in range(connections_count)]
        self._events = [threading.Event() for _ in range(connections_count)]
        # Connect first and last squads
        for worker in self._pipable_squad_list[0]:
            worker.set_output_queue(self._queues[0], self._events[0])
        for worker in self._pipable_squad_list[-1]:
            worker.set_input_queue(self._queues[-1], self._events[-1])
        # Connect all middle squads
        for i, squad in enumerate(self._pipable_squad_list[1:-1]):
            for worker in squad:
                worker.set_input_queue(self._queues[i], self._events[i])
                worker.set_output_queue(self._queues[i + 1], self._events[i + 1])

    def run(self):
        log.info("Started running pipe manager")
        self._init_pipe()
        working_futures = []
        with futures.ThreadPoolExecutor(self.MAX_WORKERS) as executor:
            for squad in self._pipable_squad_list:
                for worker in squad:
                    log.debug(f'Submited worker: {worker}')
                    future = executor.submit(worker.work_until_done)
                    working_futures.append(future)
            # Wait for all workers to finish before exiting with statement
            done, not_done = futures.wait(working_futures, return_when='FIRST_EXCEPTION')
            if not_done:
                self.kill()

    def shutdown(self):
        """
        Shutdown all pipes by setting all the input events.
        """
        log.warning('Performing a shutdown')
        for event in self._events:
            event.set()
    
    def kill(self):
        """
        Kills all pipes by setting all the input events and clearing all pipes
        """
        log.warning('Performing a kill')
        self.shutdown()
        for q in self._queues:
            with q.mutex:
                q.queue.clear()
