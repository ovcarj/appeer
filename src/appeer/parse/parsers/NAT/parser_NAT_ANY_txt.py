"""Text parser for any Nature journal"""

import functools
#import re

#import bs4

from appeer.general import utils as _utils

from appeer.parse.parsers.parser import Parser
#from appeer.parse.parsers import date_utils

class Parser_NAT_ANY_txt(Parser,
        publisher_code='NAT',
        journal_code='ANY',
        data_type='txt'):
    """
    Text parser for any Nature journal

    """

    @staticmethod
    def check_publisher_journal(input_data, parser='html.parser'):
        """
        Checks whether ``input_data`` corresponds to Nature

        Parameters
        ----------
        input data : bs4.BeautifulSoup | str
           Data loaded into ``BeautifulSoup`` or a path to a file to be parsed
        parser : str
            The parser used by ``BeautifulSoup``

        Returns
        -------
        is_PUBLISHER_JOURNAL : bool
            True if ``input_data`` corresponds to Nature;
                False otherwise

        """

        is_PUBLISHER_JOURNAL = False

        soup, exception = _utils.convert_2_soup(input_data, parser=parser)

        if not exception:

            pass

        return is_PUBLISHER_JOURNAL, exception

    def __init__(self, input_data, data_type='txt', parser='html.parser'):
        """
        Load the inputted data into ``self._input_data``

        Parameters
        ----------
        input_data : bs4.BeautifulSoup | str
            Data loaded into BeautifulSoup or a path to a file to be parsed
        data_type : str
            Input data type.
        parser : str
            The parser used by ``BeautifulSoup``

        """

        super().__init__(input_data=input_data,
                data_type=data_type,
                parser=parser)

    @functools.cached_property
    def doi(self):
        """
        Get the publication DOI

        Returns
        -------
        _doi : str | None
            The publication DOI

        """

        # Get the DOI from self._input_data
        # In case of failure, return None

        _doi = None

        return _doi

    @functools.cached_property
    def publisher(self):
        """
        Get the publisher

        Returns
        -------
        _publisher : str | None
            The paper publisher
        
        """

        # Get the publisher from self._input_data
        # In case of failure, return None

        _publisher = None

        return _publisher

    @functools.cached_property
    def journal(self):
        """
        Get the journal

        Returns
        -------
        _journal : str | None
            The publication journal
        
        """

        # Get the journal from self._input_data
        # In case of failure, return None

        _journal = None

        return _journal

    @functools.cached_property
    def title(self):
        """
        Get the publication title

        Returns
        -------
        _title : str | None
            Publication title
        
        """

        # Get the publication title from self._input_data
        # In case of failure, return None

        _title = None

        return _title

    @functools.cached_property
    def publication_type(self):
        """
        Get the publication type

        Returns
        -------
        _publication type : str | None
            Publication type
        
        """

        # Get the publication type from self._input_data
        # In case of failure, return None

        _publication_type = None

        return _publication_type

    @functools.cached_property
    def affiliations(self):
        """
        Get the author affiliations

        Returns
        -------
        _affiliations : list of str | None
            List of affiliations
        
        """

        # Get the affiliations from self._input_data
        # In case of failure, return None

        _affiliations = None

        return _affiliations

    @functools.cached_property
    def received(self):
        """
        Get the date when the publication was received

        Returns
        -------
        _received : str | None
            The date when the publication was received
        
        """

        # Get the publication reception date from self._input_data
        # In case of failure, return None

        _received = None

        return _received

    @functools.cached_property
    def accepted(self):
        """
        Get the date when the publication was accepted

        Returns
        -------
        _accepted : str | None
            The date when the publication was accepted
        
        """

        # Get the publication acceptance date from self._input_data
        # In case of failure, return None

        _accepted = None

        return _accepted

    @functools.cached_property
    def published(self):
        """
        Get the date when the publication was published

        Returns
        -------
        _published : str | None
            The date when the publication was published
        
        """

        # Get the publication date from self._input_data
        # In case of failure, return None

        _published = None

        return _published
