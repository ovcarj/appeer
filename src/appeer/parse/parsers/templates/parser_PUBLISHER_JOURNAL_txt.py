"""A template for a text parser

To implement a parser:

    (1a) Replace PUBLISHER and JOURNAL with appropriate names
    (1b) Rename this module accordingly and put in in
            parsers/PUBLISHER directory

    (2) Implement the check_publisher_journal method
    
    (3) Implement all the functools.cached_property methods

For more detailed documentation, check the documentation of the Parser abstract
    parent class, found in appeer.parse.parsers.parser.py

The modules which are likely to be useful, but are not strictly necessary
    are commented out

"""

import functools
#import re

#import bs4

#from appeer.general import utils as _utils

from appeer.parse.parsers.parser import Parser
#from appeer.parse.parsers import date_utils
from appeer.parse.parsers import soup_utils

class Parser_PUBLISHER_JOURNAL_txt(Parser,
        publisher_code='PUBLISHER',
        journal_code='JOURNAL',
        data_type='txt'):
    """
    Text parser for PUBLISHER, JOURNAL

    """

    @staticmethod
    def check_publisher_journal(input_data, parser='html.parser'):
        """
        Checks whether ``input_data`` corresponds to PUBLISHER, JOURNAL

        Parameters
        ----------
        input data : bs4.BeautifulSoup | str
           Data loaded into ``BeautifulSoup`` or a path to a file to be parsed
        parser : str
            The parser used by ``BeautifulSoup``

        Returns
        -------
        is_PUBLISHER_JOURNAL : bool
            True if ``input_data`` corresponds to PUBLISHER, JOURNAL;
                False otherwise

        """

        is_PUBLISHER_JOURNAL = False

        soup, exception = soup_utils.convert_2_soup(input_data, parser=parser)

        if not exception:

            # Perform a check of the soup;
            # i.e., set is_PUBLISHER_JOURNAL = True if some
            # appropriate condition(s) is fulfilled

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
            Input data type
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
    def no_of_authors(self):
        """
        Get the number of publication authors

        Returns
        -------
        _no_of_authors : int | None
            Number of publication authors

        """

        # Get the number of publication authors from self._input_data
        # In case of failure, return None

        _no_of_authors = None

        return _no_of_authors

    @functools.cached_property
    def affiliations(self):
        """
        Get the author affiliations

        Returns
        -------
        _affiliations : list of list of str | None
            List of affiliations;
                each entry corresponds to one or more affiliation(s)
                of a single author
        
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

    @functools.cached_property
    def normalized_received(self):
        """
        Normalize the ``received`` date to ISO format (YYYY-MM-DD)

        Returns
        -------
        _normalized_received : str
            Date in YYYY-MM-DD format

        """

        # Get the normalized date from self.received
        # In case of failure, a normalize_* function from date_utils
        #   should raise an error
        #
        # E.g.:
        #
        #_normalized_received = date_utils.normalize_d_M_y(self.received)
        #

        _normalized_received = None

        return _normalized_received

    @functools.cached_property
    def normalized_accepted(self):
        """
        Normalize the ``accepted`` date to ISO format (YYYY-MM-DD)

        Returns
        -------
        _normalized_accepted : str
            Date in YYYY-MM-DD format

        """


        # Get the normalized date from self.accepted
        # In case of failure, a normalize_* function from date_utils
        #   should raise an error
        #
        # E.g.:
        #
        #_normalized_accepted = date_utils.normalize_d_M_y(self.accepted)
        #

        _normalized_accepted = None

        return _normalized_accepted

    @functools.cached_property
    def normalized_published(self):
        """
        Normalize the ``published`` date to ISO format (YYYY-MM-DD)

        Returns
        -------
        _normalized_published : str
            Date in YYYY-MM-DD format

        """

        # Get the normalized date from self.published
        # In case of failure, a normalize_* function from date_utils
        #   should raise an error
        #
        # E.g.:
        #
        #_normalized_published = date_utils.normalize_d_M_y(self.published)
        #

        _normalized_published = None

        return _normalized_published
