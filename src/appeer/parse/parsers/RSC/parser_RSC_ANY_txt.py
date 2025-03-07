"""Parser for any Royal Society of Chemistry (RSC) journal"""

import functools
import re

import bs4

from appeer.general import utils as _utils

from appeer.parse.parsers.parser import Parser
from appeer.parse.parsers import date_utils
from appeer.parse.parsers import soup_utils

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

        soup, exception = soup_utils.convert_2_soup(input_data, parser=parser)

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

            _doi_candidate = soup_utils.get_meta_content(
                    soup=self._input_data,
                    attr_value='citation_doi',
                    )

            if _doi_candidate:

                _doi = _utils.get_doi_substring(str(_doi_candidate))

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
                attr_value='DC.publisher')

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
                attr_value='DC.title')

        return _title

    @functools.cached_property
    def publication_type(self):
        """
        Get the publication type

        During testing RSC parsing, two formats of XML files were encountered;
            therefore, the two cases are taken into account below

        Returns
        -------
        _publication type : str | None
            Publication type
        
        """

        _publication_type = None

        # RSC XML type (1)

        try:

            _candidate = self._input_data\
                    .find('div', class_='article_info')\
                    .find('a')\
                    .next_sibling

            if _candidate and isinstance(
                    _candidate, bs4.NavigableString):

                # Clean up string from new lines and brackets

                _cleanup = _candidate.strip()

                if _cleanup.startswith('('):
                    _cleanup = _cleanup[1:]

                if _cleanup.endswith(')'):
                    _cleanup = _cleanup[:-1]

                _publication_type = _cleanup

        # RSC XML type (2)

        except AttributeError:

            try:

                _candidate = self._input_data.\
                        find('dd', class_='c__14').text

            except AttributeError:
                pass

            else:
                _publication_type = _candidate or None

        return _publication_type

    @functools.cached_property
    def affiliations(self):
        """
        Get the author affiliations

        During testing RSC parsing, two formats of XML files were encountered;
            therefore, the two cases are taken into account below

        Returns
        -------
        _affiliations : list of list of str | None
            List of affiliations;
                each entry corresponds to one or more affiliation(s)
                of a single author
        
        """

        _affiliations = None

        #
        # RSC XML type (1)
        #

        author_sups = None
        sup_aff_map = None

        # Get superscripts which denote the affiliations of all the authors

        try:

            author_sup_tags = self._input_data.\
                    find('div', class_='article__authors').\
                    find_all('sup')

        except AttributeError:
            pass

        else:

            if author_sup_tags:

                author_sups = [list(sup.text) for sup in author_sup_tags]

                # Get a map from superscripts to affiliations

                try:

                    aff_tags = self._input_data.find_all(
                        'p', class_='article__author-affiliation')

                except AttributeError:
                    pass

                else:

                    if aff_tags:

                        sup_aff_map = {
                                tag.sup.text.strip(): tag.find('span').\
                                        find_next('span').text.strip()
                                for tag in aff_tags if tag.sup
                                }

        #
        # RSC XML type (2)
        #

        if not (author_sups and sup_aff_map):

            # Get superscripts which denote the affiliations of all the authors

            author_sup_tags = self._input_data.\
                    find_all('span', class_=re.compile(r'sup_ref .*'))

            if not author_sup_tags:
                return None

            try:
                author_sups = [list(sup.text) for sup in author_sup_tags]

            except AttributeError:
                return None

            # Get a map from superscripts to affiliations

            aff_tags = self._input_data.find_all('a', id=re.compile(r'aff.'))

            if not aff_tags:
                return None

            try:

                sup_aff_map = {
                        tag.text.strip(): tag.find_next('span')\
                                .find_next('span')\
                                .text.strip()
                        for tag in aff_tags
                    }

            except AttributeError:
                return None

        #
        # Finally, get the affiliations
        # using the author superscripts
        # and the superscript -> affiliation map
        #

        _affiliations = [
            [sup_aff_map[sup] for sup in author_sup]
            for author_sup in author_sups
            ]

        #
        # Clean up emails and new lines from affiliations
        #

        _affiliations = [
                [aff.split('\n')[0].\
                        split('E-mail')[0].\
                        strip()
                        for aff in aff_entry]
                for aff_entry in _affiliations
                ]

        return _affiliations

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
        _published : str | None
            The date when the publication was published
        
        """

        _published = None

        # RSC XML type (1)

        try:

            candidate = self._input_data.find('p',
                    class_='bold italic').text

            if 'First published' in candidate:

                _published_list = date_utils.get_d_M_y(candidate)

                if _published_list:
                    _published = _published_list[0]

        # RSC XML type (2)

        except AttributeError:

            try:

                divs = self._input_data.find_all('div', class_='c fixpadt--l')

                if not divs:
                    return _published

                for div in divs:

                    if 'First published' in div.dt.text:

                        _published_list = date_utils.get_d_M_y(div.dd.text)

                        if _published_list:
                            _published = _published_list[0]
                            break

                else:
                    pass

            except AttributeError:
                pass

        return _published
