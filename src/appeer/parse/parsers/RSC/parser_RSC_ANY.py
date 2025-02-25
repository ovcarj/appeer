"""Parser for any Royal Society of Chemistry (RSC) journal"""

from appeer.general import utils as _utils
from appeer.parse.parsers.parser import Parser

class Parser_RSC_ANY(Parser, publisher_code='RSC', journal_code='ANY'):
    """
    Parser for any Royal Society of Chemistry (RSC) journal

    """

    @staticmethod
    def check_publisher_journal(input_data):
        """
        Checks whether ``input_data`` corresponds to RSC

        Parameters
        ----------
        input data : bs4.BeautifulSoup | str
           Data loaded into ``BeautifulSoup`` or a path to a file to be parsed

        Returns
        -------
        is_RSC_ANY : bool
            True if ``input_data`` corresponds to RSC, False otherwise

        """

        is_RSC_ANY = False

        soup, exception = _utils.convert_2_soup(input_data)

        if not exception:

            try:

                if 'RSC Publishing' in soup.title.text:
                    is_RSC_ANY = True

            except AttributeError:
                pass

        return is_RSC_ANY, exception

    def __init__(self, input_data):
        """
        Load the inputted data into ``self._input``

        Parameters
        ----------
        input_data : bs4.BeautifulSoup | str
            Data loaded into BeautifulSoup or a path to a file to be parsed

        """

        super().__init__(input_data=input_data)
