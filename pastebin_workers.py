import requests
import logging
from collections import namedtuple
from urllib.parse import urlparse, urljoin
from lxml import etree
import arrow
from pipeable_worker import PipeableWorker, RetryException


BASE_URL = 'https://pastebin.com'

log = logging.getLogger('PastebinCrawler')

_PasteBase = namedtuple(
    'Paste', ['id', 'author', 'title', 'timestamp', 'content'])


class Paste(_PasteBase):
    """
    Saves the data of a single paste
    """

    def __repr__(self):
        return f'<{self.id}, {self.author}, "{self.title}", ' \
               f'{arrow.Arrow.fromtimestamp(self.timestamp).format()}>'


class RequestWorker(PipeableWorker):
    """
    This base worker can be used by any worker that crawls the web.
    Input: URL
    Output: The content of the url's website
    """
    METHOD = 'GET'
    FOLOWTHROUGH_EXCEPTIONS = (requests.RequestException,)
    RETRY_STATUS_CODES = [429]

    def __init__(self, worker_name=None, session=None):
        """
        :param worker_name: A name to be used in log messages.
                    Default to the class name.
        :param session: An optional requests like session object.
                        Will initiate one by default.
        """
        super().__init__(worker_name)
        self._session = requests.session() if session is None else session

    def work(self, url):
        super().work(url)
        try:
            res = self.request(url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in self.RETRY_STATUS_CODES:
                log.warning(
                    f'{self}: Failed request to {e.request.url} '
                    f'(code {e.response.status_code}), Retrying.')
                raise RetryException()
            # Don't propagate the error down the pipe
            return
        return self.parse(res)

    def request(self, url):
        log.debug(f'{self}: Sending {self.METHOD} request to {url}')
        res = self._session.request(self.METHOD, url)
        res.raise_for_status()
        return res

    def parse(self, res):
        """
        Parses requests.models.Response into content
        """
        return res.content


class InitPastebinWorker(RequestWorker):
    """
    This worker perform the first request to pastebin in order to get a list
    of paste ids.
    It can also be used as the first in the pipe.
    Input: Ignores
    Output: Multiple paste ids
    """

    ARCHIVE_URL = BASE_URL + '/archive'
    MAINTABLE_XPATH = "//table[@class='maintable']"
    PASTE_HREF_XPATH = "//span[contains(@class, 'public')]/../a/@href"
    STRIP_CHARS = '/'

    def work(self, _):
        """
        Ignores input, always uses the same url
        """
        log.info(f"{self}: Getting current paste ids' from archive")
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
        hrefs = table.xpath(self.PASTE_HREF_XPATH)
        paste_ids = (href.strip(self.STRIP_CHARS) for href in hrefs)
        # Add paste_ids manually to queue in order to add more than one element
        for paste_id in paste_ids:
            self._add_to_out_queue(paste_id)


class SinglePastebinWorker(RequestWorker):
    """
    This worker requests and parses pastebin for more information about a paste
    Input: Paste id
    Output: Paste object
    """
    AUTHOR_XPATH = "//div[@class='username']/a"
    TITLE_XPATH = "//div[@class='info-top']/h1"
    CONTENT_XPATH = "//div[@class='content']/*/textarea[@class='textarea']"
    DATE_XPATH = "//div[@class='date']/span"
    DATE_FORMAT = 'MMM Do, YYYY'
    TIME_XPATH = "//div[@class='date']/span/@title"
    TIME_FORMAT = 'HH:mm:ss A'
    URL_STRIP_CHARS = '/'

    def work(self, paste_id):
        url = urljoin(BASE_URL, paste_id)
        log.info(f"{self}: Getting more information about paste {paste_id}")
        return super().work(url)

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
        datetime = date.replace(
            hour=time.hour, minute=time.minute, second=time.second)
        return Paste(paste_id, author, title, datetime.timestamp, content)
