"""Parse a single file"""

from appeer.jobs.action import Action

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
        Internal publisher code; mutable
    journal : str
        Internal journal code; mutable
    title : str
        Title of the publication; mutable
    affiliations : str
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
