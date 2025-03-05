"""Determine which parser to use for a given file"""

import os
import importlib
import inspect

import bs4

from appeer.general import utils as _utils

class Preparser:
    """
    Determine which parser to use for a given file

    """

    def __init__(self,          #pylint:disable=too-many-branches
            filepath,
            publishers=None,
            journals=None,
            data_types='txt'):
        """
        Creates the preparsing strategy; loads the input file accordingly

        To determine which parser should be used for the given input file,
            the ``Preparser`` will invoke the ``check_publisher_journal``
            method of a list of candidate parsers defined as described below.

        The final selected parser will be the one for which
            ``check_publisher_journal`` returns (True, None).

        The list of all available parsers is given in
            ./implemented_parsers.json

        By default, all of the available parsers will be tried by
            ``Preparser``. A subset of the available parsers may
            be chosen by passing the ``publishers``, ``journals``
            and ``data_types`` arguments.

        E.g., ``publishers=['RSC', 'NAT']`` will attempt to use only the
            parsers with ``RSC`` and ``NAT`` publisher codes.

        The ``data_types`` argument is currently a placeholder;
            the only supported data type is ``"txt"``.

        Parameters
        ----------
        filepath : str
            Path to a file for which an appropriate parser will be determined
        publishers : str | list of str | None
            List of candidate parser publisher codes
        journals : str | list of str | None
            List of candidate parser journal codes
        data_types : str | list of str | None
            List of candidate parser data types;
                currently, only 'txt' is supported

        """

        # Check the input arguments

        if filepath and not _utils.is_list_of_str(filepath):
            raise TypeError('Invalid "filepath" argument passed to Preparser; must be a string or a list of strings.')

        if publishers and not _utils.is_list_of_str(publishers):
            raise TypeError('Invalid "publishers" argument passed to Preparser; must be a string or a list of strings.')

        if journals and not _utils.is_list_of_str(journals):
            raise TypeError('Invalid "journals" argument passed to Preparser; must be a string or a list of strings.')

        if data_types not in ('txt', ['txt']):
            raise NotImplementedError('Only "txt" data type is currently implemented.')

        filepath = _utils.abspath(filepath)

        if not _utils.file_list_readable(filepath)\
                or not _utils.file_exists(filepath):
            raise ValueError(f'The inputed file ({filepath}) does not exist or is not readable.')

        # If the arguments are passed as str, convert them to list

        if isinstance(publishers, str):
            publishers = [publishers]

        if isinstance(journals, str):
            journals = [journals]

        if isinstance(data_types, str):
            data_types = [data_types]

        # Filter parsers dictionary

        path_2_json = os.path.join(f'{os.path.dirname(__file__)}',
                'implemented_parsers.json')

        self._parsers_dict = _utils.load_json(
                path_2_json)['implemented_parsers']

        if publishers:

            self._parsers_dict = {publisher: meta
                    for (publisher, meta) in self._parsers_dict.items()
                    if publisher in publishers
                    }

        if journals:

            self._parsers_dict = {publisher: meta
                    for (publisher, meta) in self._parsers_dict.items()
                    if meta['journal'] in journals
                    }

        if data_types:

            self._parsers_dict = {publisher: meta
                    for (publisher, meta) in self._parsers_dict.items()
                    if meta['dtype'] in data_types
                    }

        if not self._parsers_dict:
            raise ValueError('No existing parser found for the inputted (publishers, journals, data_types) combination')

        #
        # Initialize the loaded data dictionary
        #
        # The goal is to minimize the number of times the input data is
        # parsed. As different Parsers subclasses might require different
        # strategies (e.g., different BeautifulSoup parsers), prepare a
        # container for the loaded data so it can be read later as needed.
        #

        self._loaded_data = {}

        txt_parsers = {publisher: meta
                for (publisher, meta) in self._parsers_dict.items()
                if meta['dtype'] == 'txt'}

        if txt_parsers:

            #
            # txt data type encountered,
            # so initialize a corresponding empty container
            #

            self._loaded_data['txt'] = {}

            #
            # Read the file;
            # if other data types are implemented in the future,
            # possibly these lines can be un-indented
            # and self._txt_data can be renamed to something more general
            #

            with open(filepath, 'r', encoding='utf-8') as f:
                self._txt_data = f.read()

    def determine_parser(self):
        """
        Determines the appropriate parser for the given input file

        In case of success, returns the appropriate ``Parser`` subclass;
            in case of failure, returns ``None``.

        The method also returns the data loaded into the appropriate object,
            (e.g. ``bs4.BeautifulSoup``), so it can be reused for parsing
            the data without rereading the input file; in case of failure,
            returns None

        Returns
        -------
        parser : appeer...Parser_<publisher>_<journal>_<data_type> | None
            The appropriate Parser subclass; None if search failed
        loaded_data : bs4.BeautifulSoup | ? | None
            The data loaded into a BeautifulSoup object (for "txt" data type);
                ? is left as placeholder for other data types;
                None if the search failed

        """

        parser = None
        loaded_data = None

        parsers_lib = 'appeer.parse.parsers'

        for publisher, meta in self._parsers_dict.items():

            parser_code = f'{publisher}_{meta["journal"]}_{meta["dtype"]}'

            parser_module = importlib.import_module(
                    f'{parsers_lib}.{publisher}.parser_{parser_code}')

            parser_class = getattr(parser_module, f'Parser_{parser_code}')

            # Text parser case

            if meta['dtype'] == 'txt':

                #
                # Get the default BeautifulSoup parser
                # used by the "check_publisher_journal" method of
                # the Parser subclass
                #

                bs_parser = inspect.signature(
                        parser_class.check_publisher_journal).\
                        parameters['parser'].default

                # Check if the data was already loaded with bs_parser

                self._load_txt_data(bs_parser=bs_parser)

                soup = self._loaded_data['txt'][bs_parser]

                # Run the "check_publisher_journal" method

                is_publisher_journal, exception =\
                        parser_class.check_publisher_journal(
                        input_data=soup,
                        parser=bs_parser)

                if (is_publisher_journal and not exception):

                    parser = parser_class
                    loaded_data = soup

                    break

        return parser, loaded_data

    def _load_txt_data(self, bs_parser):
        """
        Loads the data using the given ``BeautifulSoup`` parser

        The data is stored in ``self._loaded_data['txt']['bs_parser']``.

        If the data was previously loaded, do nothing

        Parameters
        ----------
        bs_parser : str
            The ``BeautifulSoup`` parser used to parse the data

        """

        if bs_parser in self._loaded_data['txt']:
            pass

        else:

            #
            # Perhaps some error handling should be added here,
            # though an error is unlikely to be encountered in
            # normal usage
            #

            self._loaded_data['txt'][bs_parser] =\
                    bs4.BeautifulSoup(self._txt_data, features=bs_parser)
