"""Send requests, get and handle responses"""

import time
import requests

import click

from appeer.general import log as _log
from appeer.general.config import Config
from appeer.scrape import scrape_reports as reports

class Request:
    """
    Send requests, get and handle responses

    """

    def __init__(self, url, _queue=None):
        """
        Initializes a Request instance
    
        Parameters
        ----------
        url : str
            URL string
        _queue : queue.Queue
            If given, messages will be logged in the job log file
   
        """

        self._queue = _queue

        self.url = url

        self.success = False
        self.status = None
        self.error = None
        self.response = None

    def send(self, head=False, **kwargs):
        """
        Sends a request to get the content of ``self.url``

        Parameters
        -------
        head : bool
            If True, get just the response header with allowed redirects
                (useful when resolving DOI)

        Keyword Arguments
        -----------------
        max_tries : int
            Maximum number of tries to get a response from an URL before
                giving up
        retry_sleep_time : float
            Time (in seconds) to wait before trying a nonresponsive URL again
        _429_sleep_time : float
            Time (in minutes) to wait if received a 429 status code

        """

        headers = requests.utils.default_headers()
        headers.update({'User-Agent': 'My User Agent 1.0'})

        scrape_defaults = Config().settings['ScrapeDefaults']

        kwargs.setdefault('max_tries',
                int(scrape_defaults['max_tries']))
        kwargs.setdefault('retry_sleep_time',
                float(scrape_defaults['retry_sleep_time']))
        kwargs.setdefault('_429_sleep_time',
                float(scrape_defaults['429_sleep_time']))

        max_tries = kwargs['max_tries']
        retry_sleep_time = kwargs['retry_sleep_time']
        _429_sleep_time = kwargs['_429_sleep_time']

        for i in range(max_tries):

            self._rprint(_log.underlined_message(f'HTTPS Request {i+1}/{max_tries}'))

            try:

                if head:
                    self.response = requests.head(self.url,
                            headers=headers,
                            timeout=30,
                            allow_redirects=True)

                else:
                    self.response = requests.get(self.url,
                            headers=headers,
                            timeout=30)

                if self.response.status_code == 429:

                    self.status = self.response.status_code
                    self.error = 'Too many requests'

                    self._rprint(reports.requests_report(self))
                    self._rprint(f'Got a 429 status code; sleeping for {_429_sleep_time} minutes and trying again...\n')
                    time.sleep(_429_sleep_time * 60)

                    continue

                try:
                    self.response.raise_for_status()

                except requests.exceptions.HTTPError as err:

                    self.success = False
                    self.error = None
                    self.status = err.response.status_code

                self.success = True
                self.error = None
                self.status = self.response.status_code

            except requests.exceptions.ConnectionError as err:

                self.success = False
                self.status = None
                self.error = type(err).__name__

            self._rprint(reports.requests_report(self))

            if self.success:
                break

            if i < (max_tries - 1):
                time.sleep(retry_sleep_time)

        else:
            self.success = False
            self._rprint('Scraping failed.\n')

    def _rprint(self, message):
        """
        Prints a ``message`` to stdout or puts it in the queue
        
        If the message is put into the queue, it will be logged in
            the job log file

        Parameters
        ----------
        message : str
            String to be printed to stdout or logged in the job log file

        """

        if self._queue:
            self._queue.put(message)

        else:
            click.echo(message)
