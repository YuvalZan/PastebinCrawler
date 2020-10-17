import sys
import requests
import logging
from collections import namedtuple
from urllib.parse import urlparse
from lxml import etree
import arrow
from pipeable_worker import PipeableWorker


log = logging.getLogger('PastebinCrawler')

_PasteBase = namedtuple('Paste', ['id', 'author', 'title', 'timestamp', 'content'])
class Paste(_PasteBase):
    """
    """
    def __repr__(self):
        return f'<{self.id}, {self.author}, "{self.title}", {arrow.Arrow.fromtimestamp(self.timestamp).format()}>'
        

class RequestWorker(PipeableWorker):
    METHOD = 'GET'
    FOLOWTHROUGH_EXCEPTIONS = (requests.RequestException,)
    RETRY_STATUS_CODES = [429]

    def __init__(self, worker_name=None, session=None):
        """
        """
        super().__init__(worker_name)
        self._session = requests.session() if session is None else session

    def work(self, url):
        super().work(url)
        try:
            res = self.request(url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in self.RETRY_STATUS_CODES:
                log.warning(f'{self}: Retrying failed request with error code {e.response.status_code} to {e.request.url}')
                self._add_to_input_queue(e.request.url)
            # Propagate the error down the pipe
            raise
        return self.parse(res)

    def request(self, url):
        res = self._session.request(self.METHOD, url)
        res.raise_for_status()
        return res

    def parse(self, res):
        """
        Parses requests.models.Response into content
        """
        return res.content

class InitPastebinWorker(RequestWorker):
    # FOLOWTHROUGH_EXCEPTIONS = FOLOWTHROUGH_EXCEPTIONS + (,)
    BASE_URL = 'https://pastebin.com'
    ARCHIVE_URL = BASE_URL + '/archive'
    MAINTABLE_XPATH = "//table[@class='maintable']"
    PASTE_XREF_XPATH = "//span[contains(@class, 'public')]/../a/@href"

    def work(self, _):
        """
        Ignores input, always uses the same url
        """
        super().work(self.ARCHIVE_URL)

    def first_pipe_prepare(self):
        """
        If this worker is the first in the pipe than still perform one request
        """
        super().first_pipe_prepare()
        self._add_to_input_queue(None)

    def parse(self, res):
        tree = etree.HTML(res.content)
        table_search = tree.xpath(self.MAINTABLE_XPATH)
        table = table_search[0]
        lines =  table.xpath(self.PASTE_XREF_XPATH)
        urls = (self.BASE_URL + line for line in lines)
        # Add urls manually to queue in order to add more than one element
        for url in urls:
            self._add_to_out_queue(url)


class SinglePastebinWorker(RequestWorker):
    AUTHOR_XPATH = "//div[@class='username']/a"
    TITLE_XPATH = "//div[@class='info-top']/h1"
    CONTENT_XPATH = "//div[@class='content']/*/textarea[@class='textarea']"
    DATE_XPATH = "//div[@class='date']/span"
    DATE_FORMAT = 'MMM Do, YYYY'
    TIME_XPATH = "//div[@class='date']/span/@title"
    TIME_FORMAT = 'HH:mm:ss A'
    URL_STRIP_CHARS = '/'
    
    def parse(self, res):
        paste_id = urlparse(res.url).path.strip(self.URL_STRIP_CHARS)
        tree = etree.HTML(res.content)
        author = tree.xpath(self.AUTHOR_XPATH)[0].text
        title = tree.xpath(self.TITLE_XPATH)[0].text
        content = tree.xpath(self.CONTENT_XPATH)[0].text
        # parse datetime
        raw_date = tree.xpath(self.DATE_XPATH)[0].text
        date = arrow.get(raw_date, self.DATE_FORMAT)
        raw_time = tree.xpath(self.TIME_XPATH)[0]
        time = arrow.get(raw_time, self.TIME_FORMAT)
        datetime = date.replace(hour=time.hour, minute=time.minute, second=time.second)
        return Paste(paste_id, author, title, datetime.timestamp, content)
