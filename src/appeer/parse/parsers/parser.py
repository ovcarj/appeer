"""Base abstract Parser class"""

import abc
import os
import inspect
import functools

from appeer.general import utils as _utils

from appeer.parse.parsers import soup_utils
from appeer.parse.default_metadata import default_metadata

class Parser(abc.ABC):
    """
    Base abstract Parser class

    Steps to implement a parser for a (publisher, journal) pair:

    (1) Adhere to the naming convention

            (a) Choose unique publisher and journal codes
            (b) Place the parser module in ./<publisher_code> directory
            (c) Name the module
                    parser_<publisher_code>_<journal_code>_<data_type>.py
            (d) Name the parser subclass
                    Parser_<publisher_code>_<journal_code>_<data_type>
    
            * Commonly, the same parser can be used for all journals of a given
                publisher. In that case, choose ``journal_code = ANY``.
    
            * Currently, only ``data_type == 'txt'`` is supported.
    
            Examples of the naming convention:
    
            Module                     Class
            ---------------------------------------------
            parser_RSC_ANY_txt.py      Parser_RSC_ANY_txt
            parser_NAT_ANY_txt.py      Parser_NAT_ANY_txt
            parser_APS_PRL_txt.py      Parser_APS_PRL_txt

    (2) Define a static method ``check_publisher_journal(input_data)``
    
            This method checks if the ``input_data`` corresponds to
            the (publisher, journal) pair of the parser. The method is
            called by ``Preparser`` to automatically find which parser
            should be used.

            If the parser is intended to parse text, the type
            of ``input_data`` *must* be allowed to be ``BeautifulSoup``.
            Most likely, it should also allowed to be a ``str``, so a
            filepath can be passed.

            The check_publisher_journal method must return a boolean and
                an exception (if reading of ``input_data`` failed):

                is_publisher_journal : bool
                    True if ``input_data`` corresponds to the
                        (publisher, journal) pair; False otherwise
                exception : None | type
                    The exception that was raised due to reading failure

            An appropriate function for loading text data is

            soup, exception =\
                    appeer.general.utils.convert_2_soup(input_data, parser)

            Pseudocode example
            ------------------

            @staticmethod
            def check_publisher_journal(input_data):

                # Read data

                soup, exception = convert_2_soup(input_data)

                # Check for (publisher, journal) match

                if not exception:

                    if is_publisher(soup) and is_journal(soup):
                        is_publisher_journal = True

                    else:
                        is_publisher_journal = False

                return is_publisher_journal, exception

    (3) [Optional] Redefine the list of metadata properties that have to
            be parsed for the parsing process to be considered successful.

            Possibly, certain metadata properties are not available for some
            (publisher, journal) pairs, so list only the available properties
            so the parsing can be flagged as successful.

            This list is defined in the ``_define_metadata_list`` class
            method.

    (4) Implement the necessary cached properties

            All the properties listed by ``_define_metadata_list`` must be
            implemented as ``functools.cached_property``.

            For more details, see the documentation of
            ``_define_metadata_list``

    (5) Add the (publisher, journal) pair to ./implemented_parsers.json
            for the parser to be recognized by Preparser.

    (6) Add a normalized (standard) publisher name to ./publishers_index.json,
            along with possible publisher name variants

    (7) Add normalized (standard) journals names to
            ./<publisher_code>/<publisher_code>_journals.json,
            along with possible journal name variants

    """

    @classmethod
    def _define_metadata_list(cls):
        """
        Defines a list of metadata properties that a parser implements

        Parsing will be considered successful only if all of the listed
            metadata properties are parsed from the publication.

        The default list of the properties is defined in
            appeer.parse.default_metadata.default_metadata()

        If it is not possible to parse a certain property for a given
            (publisher, journal), this method should be redefined in the
            parser subclass.

        All properties defined in this list must be implemented as a
            ``functools.cached_property``. E.g., for the ``doi`` property,
            the parser subclass should implement a method that looks like:

            @functools.cached_property
            def doi(self):

                # Method documentation
                ...

                # Parse the DOI from the publication

                _doi = find_doi(self._input_data)

                return _doi

            ...and similarly for all the other properties in the metadata list

        """

        cls.metadata_list = default_metadata()

    def __init_subclass__(cls, publisher_code, journal_code, data_type): #pylint:disable=too-many-locals, too-many-branches
        """
        Ensures that every Parser subclass is correctly defined

        Check the class documentation for more details

        Parameters
        ----------
        publisher_code : str
            Internal unique publisher identifier
        journal_code : str
            Internal unique journal identifier
        data_type : str
            The type of the input data. Currently, only "txt" is implemented

        """

        #
        # Check if the naming convention was respected in module name
        #

        module_name = os.path.basename(inspect.getfile(cls))

        module_prefix, module_publisher, module_journal, module_data_type =\
                module_name.split('.')[0].split('_')

        if not module_prefix == 'parser':
            raise ValueError(f'Naming convention not respected: module prefix "{module_prefix}" is not equal to "parser" in {module_name}')

        if not module_publisher == publisher_code:
            raise ValueError(f'Naming convention not respected: publisher code "{publisher_code}" is not equal to "{module_publisher}" in {module_name}')

        if not module_journal == journal_code:
            raise ValueError(f'Naming convention not respected: journal code "{journal_code}" is not equal to "{module_journal}" in {module_name}')

        if not module_data_type == data_type:
            raise ValueError(f'Naming convention not respected: data type "{data_type}" is not equal to "{module_data_type}" in {module_name}')

        #
        # Check if the naming convention was respected in the class name
        #

        class_prefix, class_publisher, class_journal, class_data_type =\
                cls.__name__.split('_')

        if not class_prefix == 'Parser':
            raise ValueError(f'Naming convention not respected: class prefix "{class_prefix}" is not equal to "Parser" in class {cls.__name__}')

        if not class_publisher == publisher_code:
            raise ValueError(f'Naming convention not respected: publisher code "{publisher_code}" is not equal to "{class_publisher}" in class {cls.__name__}')

        if not class_journal == journal_code:
            raise ValueError(f'Naming convention not respected: journal code "{journal_code}" is not equal to "{class_journal}" in class {cls.__name__}')

        if not class_data_type == data_type:
            raise ValueError(f'Naming convention not respected: data type "{data_type}" is not equal to "{class_data_type}" in class {cls.__name__}')

        #
        # Check if ``check_publisher_journal`` static method was implemented
        #
        # This approach is chosen here instead of adding a static method to
        # this abstract class that raises NotImplementedError, because in that
        # case the error would be raised only when the object is instantiated,
        # and we want to the preparser to use the static method before
        # instantiation
        #

        try:
            check_implemented = isinstance(
                    cls.__dict__['check_publisher_journal'],
                    staticmethod)

        except KeyError as exc:
            raise NotImplementedError(f'The check_publisher_journal static method is not implemented in class {cls.__name__}') from exc

        if not check_implemented:
            raise NotImplementedError(f'The check_publisher_journal method in class {cls.__name__} must be declared as static.')

        #
        # Check if all defined metadata properties are implemented as
        # functools.cached_property
        #

        cls._define_metadata_list()

        for entry in cls.metadata_list:

            #
            # normalized_publisher and normalized_journal are implemented
            # in this (parent) class
            #

            if entry in ('normalized_publisher', 'normalized_journal'):
                continue

            try:
                is_cached_property = isinstance(cls.__dict__[entry],
                        functools.cached_property)

            except KeyError as exc:
                raise NotImplementedError(f'The "{entry}" cached property is not implemented in {cls.__name__}.') from exc

            if not is_cached_property:
                raise TypeError(f'"{entry}" must be implemented as functools.cached_property in {cls.__name__}')

    def __init__(self,                  #pylint:disable=too-many-arguments
            input_data,
            data_type='txt',
            parser='html.parser',
            publishers_index=None,
            publisher_journals=None):
        """
        Load the data to be parsed to ``self._input_data``

        Currently, only text parsing is implemented. The ``data_type`` input
            parameter is left as a placeholder so other data types can be
            implemented in the future.

        If ``data_type == txt``, the ``input_data`` parameter may be passed
            as a ``str`` or a ``bs4.BeautifulSoup`` object.

            In case ``isinstance(input_data, str)``, it should be a path to
            a file to be parsed.

            Otherwise, the file may be previously read and passed as a
            ``bs4.BeautifulSoup`` object. This is the expected behavior,
            as usually the data was previously read by ``Preparser``

        Parameters
        ----------
        input_data : bs4.BeautifulSoup | str
            Data loaded into ``BeautifulSoup`` or a path to a file to be parsed
        data_type : str
            Input data type. Currently, only "txt" is supported
        parser : str
            In case of "txt" data type, the parser used by ``BeautifulSoup``
        publishers_index : dict | None
            The ./publishers_index.json file loaded to a dict
                If None, it will be loaded
        publisher_journals : dict | None
            ./<publisher_code>/<publisher_code>_journals.json loaded to a dict
                If None, it will be loaded

        """

        self._input_data = None
        self.reading_exception = None

        if data_type == 'txt':

            self._input_data, self.reading_exception =\
                    soup_utils.convert_2_soup(input_data, parser=parser)

        else:
            raise NotImplementedError('Currently, only text parsing is implemented.')

        if publishers_index:

            if not isinstance(publishers_index, dict):
                raise TypeError('The inputted "publishers_index" is invalid; must be a dict.')

            self._publishers_index = publishers_index

        else:

            parsers_dir = os.path.dirname(
                    inspect.getfile(self.__class__.__base__)
                    )

            self._publishers_index = _utils.load_json(
                    os.path.join(parsers_dir, 'publishers_index.json')
                    )

        if publisher_journals:

            if not isinstance(publisher_journals, dict):
                raise TypeError('The inputted "publisher_journals" is invalid; must be a dict.')

            self._publisher_journals = publisher_journals

        else:

            parser_dir = os.path.dirname(
                    inspect.getfile(self.__class__)
                    )

            publisher_code = self.__class__.__name__.split('_')[1]

            publisher_journals_json = f'{publisher_code}_journals.json'

            self._publisher_journals = _utils.load_json(
                    os.path.join(parser_dir, publisher_journals_json)
                    )

    @functools.cached_property
    def normalized_publisher(self):
        """
        Obtains the normalized publisher name from the parsed publisher name

        The normalized publisher names are defined in ./publishers_index.json

        Returns
        -------
        _normalized_publisher : str | None
            The normalized publisher name; None if normalization failed

        """

        if not self.publisher: #pylint:disable=no-member
            return None

        _normalized_publisher = None

        for publisher_data in self._publishers_index.values():

            for variant in publisher_data['name_variants']:

                ratio = _utils.compare_strings(self.publisher, variant) #pylint:disable=no-member

                if ratio > 0.9:

                    _normalized_publisher = publisher_data['normalized_name']
                    break

            if _normalized_publisher:
                break

        return _normalized_publisher

    @functools.cached_property
    def normalized_journal(self):
        """
        Obtains the normalized journal name from the parsed journal name

        The normalized journal names are defined in
            ./<publisher_code>/<publisher_code>_journals.json

        Returns
        -------
        _normalized_journal : str | None
            The normalized journal name; None if normalization failed

        """

        if not self.journal: #pylint:disable=no-member
            return None

        _normalized_journal = None

        for journal_data in self._publisher_journals.values():

            for variant in journal_data['name_variants']:

                ratio = _utils.compare_strings(self.journal, variant) #pylint:disable=no-member

                # Ratio arbitrarily chosen to be strict; may require testing
                if ratio > 0.97:

                    _normalized_journal = journal_data['normalized_name']
                    break

            if _normalized_journal:
                break

        return _normalized_journal

    @property
    def success(self):
        """
        A flag describing whether the whole parsing procedure was successful

        The cached properties, defined by ``self.metadata_list`` are checked.
            If they are all not None, the parsing procedure is
            considered a success

        Returns
        -------
        _success : bool
            True if parsing was successful, False otherwise

        """

        _success = all(getattr(self, entry) for entry in self.metadata_list)

        return _success

    @success.setter
    def success(self, value):
        """
        This attribute should never be directly set

        """

        raise PermissionError('Cannot directly set the "success" attribute.')
