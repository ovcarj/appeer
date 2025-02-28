"""Parser for any Royal Society of Chemistry (RSC) journal"""

import functools

from appeer.general import utils as _utils

from appeer.parse.parsers.parser import Parser
from appeer.parse.parsers import date_utils

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
        Load the inputted data into ``self._input_data``

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

    def _get_meta_content(self, meta_name):
        """
        Get the content of the <meta> tag with ``meta_name`` attribute

        Useful for several metadata properties in RSC journals.

        If not successful, return None

        Parameters
        ----------
        meta_name : str
            The attribute to search for in the <meta> tags

        Returns
        -------
        content : str | None
            The value of the ``content`` attribute

        """

        content = None

        _meta = self._input_data.find('meta', attrs={'name': meta_name})

        if _meta:

            try:

                candidate = _meta.attrs['content']
                content = candidate or None

            except KeyError:
                pass

        return content

    @functools.cached_property
    def doi(self):
        """
        Get the publication DOI

        During testing RSC parsing, two formats of XML files were encountered;
            therefore, the two cases are taken into account below

        Returns
        -------
        _doi : str | None
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
        _publisher : str | None
            The paper publisher
        
        """

        _publisher = self._get_meta_content(meta_name='DC.publisher')

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

        _journal = self._get_meta_content(meta_name='citation_journal_title')

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

        _title = self._get_meta_content(meta_name='DC.title')

        return _title

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

        During testing RSC parsing, two formats of XML files were encountered;
            therefore, the two cases are taken into account below

        Returns
        -------
        _received : str | None
            The date when the publication was received
        
        """

        _received = None

        # RSC XML type (1)

        try:

            candidate = self._input_data.find('span',
                    class_='italic bold').text

            if 'Received' in candidate:

                _received_list = date_utils.get_d_M_y(candidate)

                if _received_list:
                    _received = _received_list[0]

        # RSC XML type (2)

        except AttributeError:

            try:

                divs = self._input_data.find_all('div', class_='c fixpadt--l')

                if not divs:
                    return _received

                for div in divs:

                    if 'Submitted' in div.dt.text:

                        _received_list = date_utils.get_d_M_y(div.dd.text)

                        if _received_list:
                            _received = _received_list[0]
                            break

                else:
                    pass

            except AttributeError:
                pass

        return _received

    @functools.cached_property
    def accepted(self):
        """
        Get the date when the publication was accepted

        During testing RSC parsing, two formats of XML files were encountered;
            therefore, the two cases are taken into account below

        Returns
        -------
        _accepted : str | None
            The date when the publication was accepted
        
        """

        _accepted = None

        # RSC XML type (1)

        try:

            candidate = self._input_data.find('span',
                    class_='bold italic').text

            if 'Accepted' in candidate:

                _accepted_list = date_utils.get_d_M_y(candidate)

                if _accepted_list:
                    _accepted = _accepted_list[0]

        # RSC XML type (2)

        except AttributeError:

            try:

                divs = self._input_data.find_all('div', class_='c fixpadt--l')

                if not divs:
                    return _accepted

                for div in divs:

                    if 'Accepted' in div.dt.text:

                        _accepted_list = date_utils.get_d_M_y(div.dd.text)

                        if _accepted_list:
                            _accepted = _accepted_list[0]
                            break

                else:
                    pass

            except AttributeError:
                pass

        return _accepted

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
