"""Parser for any Royal Society of Chemistry (RSC) journal"""

import functools

from appeer.general import utils as _utils
from appeer.parse.parsers.parser import Parser

class Parser_RSC_ANY_txt(Parser,
        publisher_code='RSC',
        journal_code='ANY',
        data_type='txt'):
    """
    Parser for any Royal Society of Chemistry (RSC) journal

    """

    @staticmethod
    def check_publisher_journal(input_data, parser='html.parser'):
        """
        Checks whether ``input_data`` corresponds to RSC

        Parameters
        ----------
        input data : bs4.BeautifulSoup | str
           Data loaded into ``BeautifulSoup`` or a path to a file to be parsed
        parser : str
            The parser used by ``BeautifulSoup``

        Returns
        -------
        is_RSC_ANY : bool
            True if ``input_data`` corresponds to RSC, False otherwise

        """

        is_RSC_ANY = False

        soup, exception = _utils.convert_2_soup(input_data, parser=parser)

        if not exception:

            try:

                if 'RSC Publishing' in soup.title.text:
                    is_RSC_ANY = True

            except AttributeError:
                pass

        return is_RSC_ANY, exception

    def __init__(self, input_data, data_type='txt', parser='html.parser'):
        """
        Load the inputted data into ``self._input``

        Parameters
        ----------
        input_data : bs4.BeautifulSoup | str
            Data loaded into BeautifulSoup or a path to a file to be parsed
        data_type : str
            Input data type. Currently, only "str" is supported
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

        During testing RSC parsing, two formats of XML files were encountered;
            therefore, the two cases are taken into account below

        Returns
        -------
        _doi : str
            The publication DOI

        """

        # RSC XML type (1)

        _doi = _utils.get_doi_substring(self._input_data.title.text)

        # RSC XML type (2)

        if not _doi:

            _meta = self._input_data.find('meta',
                    attrs={'name': 'citation_doi'})

            if _meta:

                try:
                    _doi_candidate = _meta.attrs['content']
                    _doi = _utils.get_doi_substring(str(_doi_candidate))

                except KeyError:
                    pass

        return _doi

    @functools.cached_property
    def publisher(self):
        """
        Get the publisher

        Returns
        -------
        _publisher : str
            The paper publisher
        
        """

        return None

    @functools.cached_property
    def journal(self):
        """
        Get the journal

        Returns
        -------
        _journal : str
            The publication journal
        
        """

        return None

    @functools.cached_property
    def title(self):
        """
        Get the publication title

        Returns
        -------
        _title : str
            Publication title
        
        """

        return None

    @functools.cached_property
    def affiliations(self):
        """
        Get the author affiliations

        Returns
        -------
        _affiliations : list of str
            List of affiliations
        
        """

        return None

    @functools.cached_property
    def received(self):
        """
        Get the date when the publication was received

        Returns
        -------
        _received : str
            The date when the publication was received
        
        """

        return None

    @functools.cached_property
    def accepted(self):
        """
        Get the date when the publication was accepted

        Returns
        -------
        _accepted : str
            The date when the publication was accepted
        
        """

        return None

    @functools.cached_property
    def published(self):
        """
        Get the date when the publication was published

        Returns
        -------
        _published : str
            The date when the publication was published
        
        """

        return None
