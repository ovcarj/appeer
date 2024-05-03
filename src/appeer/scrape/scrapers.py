import requests

import appeer.log

class Scraper:
    """
    Download data based on url, publisher and strategy (as given by ScrapePlan)
    """
    def __init__(self, url, publisher, strategy)
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
        """

        self._publisher_change = False
        self._strategy_change = False

        self.url = url
        self.publisher = publisher
        self.strategy = strategy

        self._headers = _initialize_headers()

        self._define_strategy_map()

        self._report = ''
        self._dashes = appeer.log.get_log_dashes()
        self._short_dashes = appeer.log.get_short_log_dashes()

    def _define_strategy_map():
        """
        Define a map (publisher, strategy) -> scrape method
        """
 
    def _initialize_headers():
        """ 
        Create a default header using ``requests.utils.default_headers()``.
    
        Returns
        -------
        headers : requests.structures.CaseInsensitiveDict
            Default requests header

        """
    
        headers = requests.utils.default_headers()
        headers.update({'User-Agent': 'My User Agent 1.0'})

        self._headers = headers
    
    def scrape_html(url):
        """
        Download HTML from an URL and store it as self.response_text.
    
        Parameters
        ----------
        url : str
            URL string
    
        """
    
        response = requests.get(url, headers=self._headers)
        # TODO: check validity of response
    
        response_text = response.text
    
        self.response_text = response_text
