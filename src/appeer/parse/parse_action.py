"""Parse a single file"""

from appeer.general import utils as _utils

from appeer.jobs.action import Action

from appeer.parse.parsers.preparser import Preparser
from appeer.parse import parse_reports as reports

class ParseAction(Action, action_type='parse'): #pylint:disable=too-many-instance-attributes
    """
    Parse a single file

    Instances of the ParseAction class have dynamically created properties
        corresponding to the columns of the ``scrapes`` table in the
        ``jobs`` database

        Some of these properties are mutable; changing their value will
        update the value in the ``jobs`` database. The values of the
        non-mutable properties are set using ``self.new_action(**kwargs)``.
        The values of these properties may be edited only if
        ``self._action_mode == 'write'

    List of the dynamically created properties:

    Dynamic Properties
    ------------------
    label : str
        Label of the job that the action corresponds to
    action_index : int
        Index of the action within the job
    scrape_label : str
        Label of the scrape job that the input file corresponds to
            (can be None if the input file does not come from a scrape job)
    scrape_action_index : int
        Index of the scrape action that the input file corresponds to
            (can be None if the input file does not come from a scrape job)
    date : str
        Date and time of the job creation/run; mutable
    input_file : str
        Path to the input file to be parsed
    doi : str
        DOI of the publication; mutable
    publisher : str
        Publisher; mutable
    journal : str
        Publication journal; mutable
    title : str
        Title of the publication; mutable
    publication_type : str
        Type of publication; mutable
    affiliations : list of str
        Author affiliations; mutable
    received : str
        Date of the publication reception; mutable
    accepted : str
        Date of the publication acceptance; mutable
    published : str
        Date of publication; mutable
    parser : str
        Name of the parser class used to parse the input file; mutable
    success : str
        Whether the action executed successfully; one of ('T', 'F'); mutable
    status : str
        Action status; one of ('I', 'W', 'R', 'E', 'X'); mutable
    committed : str
        Whether the data corresponding to this action was committed;
            one of ('T', 'F'); mutable

    """

    def __init__(self, label=None, action_index=None, action_mode='read'):
        """
        Connects to the job database and sets the action label and index

        Parameters
        ----------
        label : str
            Label of the job that the action corresponds to
        action_index : int
            Index of the action within the corresponding job

        """

        super().__init__(label=label,
                action_index=action_index,
                action_mode=action_mode)

    def new_action(self,
            parse_entry,
            label=None,
            action_index=None):
        """
        Creates a new parse action for a given parse entry

        Parameters
        ----------
        parse_entry : appeer.parse.parse_packer._ParseEntry
            Named tuple with fields
                ('scrape_label', 'scrape_action_index', 'filepath')
        label : str
            Label of the job that the action corresponds to
        action_index : int
            Index of the action within the corresponding job

        """

        self._action_mode = 'write'

        if label:
            self.label = label

        if action_index:
            self.action_index = action_index

        self._initialize_action_common(
                scrape_label=parse_entry.scrape_label,
                scrape_action_index=parse_entry.scrape_action_index,
                input_file=parse_entry.filepath,
                status='W')

    def run(self, _queue=None, **kwargs):
        """
        Run the parse action

        If ``queue`` is given, messages will be sent to the ``queue``
            and logged in the job log file

        The rest of the arguments will be passed to the Preparser.
            They may be used to accelerate determining the appropriate
            parser for the given input file.

        For more details, see the documentation of the Preparser class

        Parameters
        ----------
        queue : queue.Queue
            If given, messages will be logged in the job log file

        Keyword Arguments
        -----------------
        publishers : str | list of str | None
            List of candidate parser publisher codes
        journals : str | list of str | None
            List of candidate parser journal codes
        data_types : str | list of str | None
            List of candidate parser data types;
                currently, only 'txt' is supported

        """

        start_datetime = _utils.get_current_datetime()

        self._queue = _queue

        self._action_mode = 'write'
        self.status = 'R'

        self.date = start_datetime

        parser_class, loaded_data = self._determine_parser(**kwargs)

        try:
            self.parser = parser_class.__name__

        except AttributeError:
            self.parser = None
            self.success = 'F'

        self._aprint(reports.parse_action_start(self))

        if not self.parser:
            self._aprint('Preparser failed to determine the appropriate parser. Skipping this entry.\n')

        else:

            parser = parser_class(loaded_data)

            self._aprint(reports.parsing_report(parser))

            if parser.success:

                for meta in parser.metadata_list:
                    setattr(self,
                            meta,
                            getattr(parser, meta))

                self.success = 'T'

            else:
                self.success = 'F'

        self.status = 'X'

    def _determine_parser(self, **kwargs):
        """
        Determines the appropriate Parser subclass for ``self.filepath``

        Keyword Arguments
        -----------------
        publishers : str | list of str | None
            List of candidate parser publisher codes
        journals : str | list of str | None
            List of candidate parser journal codes
        data_types : str | list of str | None
            List of candidate parser data types;
                currently, only 'txt' is supported

        Returns
        -------
        parser : appeer...Parser_<PUBLISHER>_<JOURNAL>_<DATA_TYPE> | None
            The appropriate Parser subclass; None if search failed
        loaded_data : bs4.BeautifulSoup | ? | None
            The data loaded into a BeautifulSoup object (for "txt" data type);
            ? is left as placeholder for other data types;
            None if the search failed

        """

        preparser = Preparser(filepath=self.input_file,
                publishers=kwargs['publishers'],
                journals=kwargs['journals'],
                data_types=kwargs['data_types'])

        parser_class, loaded_data = preparser.determine_parser()

        return parser_class, loaded_data

    def mark_as_committed(self):
        """
        Marks the parse action as committed.

        If the action does not exist, does nothing.

        """

        if self._action_exists:

            self._action_mode = 'write'
            self.committed = 'T'

    def mark_as_uncommitted(self):
        """
        Marks the parse action as uncommitted.

        If the action does not exist, does nothing.

        """

        if self._action_exists:

            self._action_mode = 'write'
            self.committed = 'F'
