"""Text parser for any Nature journal"""

import functools
#import re

#import bs4

from appeer.general import utils as _utils

from appeer.parse.parsers.parser import Parser
from appeer.parse.parsers import date_utils
from appeer.parse.parsers import soup_utils

class Parser_NAT_ANY_txt(Parser,
        publisher_code='NAT',
        journal_code='ANY',
        data_type='txt'):
    """
    Text parser for any Nature journal

    """

    @staticmethod
    def check_publisher_journal(input_data, parser='lxml'):
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
        is_NAT_ANY : bool
            True if ``input_data`` corresponds to Nature;
                False otherwise

        """

        is_NAT_ANY = False

        soup, exception = soup_utils.convert_2_soup(input_data, parser=parser)

        if not exception:

            publisher = soup_utils.get_meta_content(
                    soup=soup,
                    attr_value='dc.publisher')

            is_NAT_ANY = 'Nature' in publisher

        return is_NAT_ANY, exception

    def __init__(self, input_data, data_type='txt', parser='lxml'):
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

    def _parse_NAT_date(self, date_type):
        """
        Parses the received/published/accepted dates from any Nature journal

        Parameters
        ----------
        date_type : str
            One of ('Received', 'Accepted', 'Published')

        Returns
        -------
        _date : str | None
            The parsed date; None if parsing failed

        """

        if date_type not in('Received', 'Accepted', 'Published'):
            raise ValueError('Invalid date_type inputted into _parse_NAT_date; must be one of ("Received", "Accepted", "Published").')

        _date = None

        li_list = self._input_data.find_all('li',
                class_='c-bibliographic-information__list-item')

        if li_list:

            for li in li_list:

                try:
                    text = li.p.text

                except AttributeError:
                    pass

                else:

                    if date_type in text:

                        try:
                            _date = date_utils.get_d_M_y(text)[0]

                        except IndexError:
                            pass

                        else:
                            break

        return _date

    @functools.cached_property
    def doi(self):
        """
        Get the publication DOI

        Returns
        -------
        _doi : str | None
            The publication DOI

        """

        _doi = soup_utils.get_meta_content(
                soup=self._input_data,
                attr_value='DOI')

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

        _publisher = soup_utils.get_meta_content(
                soup=self._input_data,
                attr_value='dc.publisher')

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

        _journal = soup_utils.get_meta_content(
                soup=self._input_data,
                attr_value='citation_journal_title')

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

        _title = soup_utils.get_meta_content(
                soup=self._input_data,
                attr_value='dc.title')

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

        _publication_type = soup_utils.get_meta_content(
                soup=self._input_data,
                attr_value='citation_article_type')

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

        _received = self._parse_NAT_date('Received')

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

        _accepted = self._parse_NAT_date('Accepted')

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

        _published = self._parse_NAT_date('Published')

        return _published
