import sys
import time
import requests

from appeer.log import get_very_short_log_dashes as short_dashes

class Request:
    """
    Sends requests, handle responses, errors, etc...

    """

    def __init__(self, url, max_tries=3, retry_sleep_time=10, _logger=None):
        """
        Initializes Request instance.
    
        Parameters
        ----------
        url : str
            URL string
        max_tries : int
            Maximum number of tries to get a response from an URL before giving up
        retry_sleep_time : float
            Time (in seconds) to wait before trying an URL again
        _logger : logging.Logger | None
            If given, logging.Logger object used to write into the logfile
    
        """
    
        self._report = ''
        self._dashes = short_dashes()

        self.url = url

        self._max_tries = max_tries
        self._retry_sleep_time = retry_sleep_time
        self._logger = _logger

        self._tries_counter = max_tries
        self._429_flag = False

        self._success = False

        self._initialize_headers()

    def send(self, head=False):
        """
        Sends a request to get the content of ``self.url``.

        Parameters
        -------
        head : bool
            If True, get just the response header with allowed redirects (for resolving DOI)
 
        """

        self._log(f'Sending a request to {self.url} ...')

        try:
            
            if head:
                self.response = requests.head(self.url, headers=self._headers, timeout=30, allow_redirects=True)

            else:
                self.response = requests.get(self.url, headers=self._headers, timeout=30) 

            try:
    
                self._check_response()
    
                self._log(f'Response OK. (Status code = {self.response.status_code})')
    
                self._success = True
    
            except:
    
                self._handle_failure()

        except:

            self._handle_failure()

    def _check_response(self):
        """
        Checks the status code of the response.

        """

        try:
            self.response.raise_for_status()

        except:

            if self.response.status_code == 429:

                self._429_flag = True
                self._handle_failure()

            else:
                # Does it make sense to retry for any other status codes?
                # For now, we immediately give up.
                self._all_requests_failed()

    def _handle_failure(self):
        """
        Handle failure of a single request due to, e.g., invalid URL, connection failure,...

        """

        response_ex_type, response_ex_message, response_ex_traceback = sys.exc_info()
        
        self._log(self._dashes)
        self._log(f'Sending a request to {self.url} failed with the following exception:')
        self._log(f'{response_ex_type.__name__}: {response_ex_message}')

        self._tries_counter -= 1

        if self._tries_counter > 0:

            if self._429_flag:

                self._log(f'Got a 429 status code. Will sleep for 5 minutes and try again. Remaining # of tries: {self._tries_counter}')
                time.sleep(5 * 60)

                self._429_flag = False

            else:

                self._log(f'Trying again in {self._retry_sleep_time} s. Remaining # of tries: {self._tries_counter}')
                time.sleep(self._retry_sleep_time)

            self.send()

        else:

            self._all_requests_failed()

    def _all_requests_failed(self):
        """
        Prints a message in the case of failure.

        """

        self._log(self._dashes)
        self._log(f'Sending a request to {self.url} failed.')
        self._success = False

    def _initialize_headers(self):
        """ 
        Creates a default header using ``requests.utils.default_headers()``.
    
        Returns
        -------
        headers : requests.structures.CaseInsensitiveDict
            Default requests header

        """
    
        headers = requests.utils.default_headers()
        headers.update({'User-Agent': 'My User Agent 1.0'})

        self._headers = headers

    def _log(self, text):
        """
        Adds text to self._report and, if self._logger exist, writes to log.

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
