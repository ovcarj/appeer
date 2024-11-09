import requests

import appeer.log

from appeer.scrape.scrape_plan import ScrapePlan
from appeer.scrape.request import Request

class Scraper:
    """
    Download data based on url, publisher and strategy (as given by ScrapePlan)

    """

    def __init__(self, url, publisher, strategy, max_tries, retry_sleep_time, _logger=None):
        """
        Initialize Scraper instance.
    
        Parameters
        ----------
        url : str
            URL string
        publisher : str
            Internal publisher code
        strategy : str
            Internal download strategy code
        max_tries : int
            Maximum number of tries to get a response from an URL before giving up
        retry_sleep_time : float
            Time (in seconds) to wait before trying an URL again
        _logger : logging.Logger | None
            If given, logging.Logger object used to write into the logfile

        """

        self.url = url
        self.publisher = publisher
        self.strategy = strategy
        self._logger = _logger

        self._request = Request(url=url, max_tries=max_tries, 
                retry_sleep_time=retry_sleep_time, _logger=_logger)

        self._define_strategy_map()

        self._report = ''
        self._dashes = appeer.log.get_log_dashes()
        self._short_dashes = appeer.log.get_short_log_dashes()
        self._very_short_dashes = appeer.log.get_very_short_log_dashes()

        self._write_text = False
        self._publisher_change = False
        self._strategy_change = False
        self.success = False

        self._get_scrape_method()

    def _define_strategy_map(self):
        """
        Define a map from publishers and strategies to scrape methods.

        scrape_method <- [[publisher1, strategy1], [publisher2, strategy2], ...]

        """

        self._strategy_map = {
                'scrape_html_simple': [['RSC', 'html'], ['Unknown', 'html']],
                'doi_handler': [['DOI', 'doi']],
                'scrape_skip': [['Invalid_URL', 'skip']]
                }

    def _get_scrape_method(self):
        """
        Use the strategy map to find the scrape method 
        for the given publisher and scrape strategy
        """

        publisher_strategy = [self.publisher, self.strategy]

        for scrape_method, pub_strat in self._strategy_map.items():

            if publisher_strategy in pub_strat:
                self.scrape_method = scrape_method
                break

    def run_scrape(self):
        """
        Run a scrape method according to self.scrape_method.
        """

        self._log(f'Method: {self.scrape_method}()')

        getattr(self, self.scrape_method)()
 
    def scrape_html_simple(self):
        """
        Download HTML from an URL and store it as self.response_text.
        """

        self._request.send()

        if self._request._success:

            self.response_text = self._request.response.text

            self._write_text = True
            self.success = True

        else:
            pass

    def scrape_skip(self):
        """
        Skip scraping because of invalid URL and/or 'skip' strategy.
        """

        self._log('Skipping this entry.')

        self.success = False

    def doi_handler(self):
        """
        Resolve DOI and update publisher/strategy accordingly, then run_scrape().
        """

        self._log('Resolving DOI...')

        self._request.send(head=True)

        if self._request._success:

            resolved_url = self._request.response.url
            self._log(f'DOI resolved to {resolved_url}')

            plan = ScrapePlan(resolved_url)

            self.publisher, self.strategy = plan.publishers[0], plan.strategies[0]
            self._publisher_change, self._strategy_change = True, True
            self._log(f'Publisher changed to {self.publisher}.')
            self._log(f'Strategy changed to {self.strategy}.')

            self._get_scrape_method()

            self.run_scrape()

        else:
            pass

    def _log(self, text):
        """
        Add text to self._report and, if self._logger exist, write to log.
    
        Parameters
        -------
        text : str
            text to write into the log
        """
    
        self._report += text + '\n'
    
        if self._logger:
            self._logger.info(text)
        
        else:
            pass
