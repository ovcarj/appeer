"""Commit parsed metadata to the ``pubs.db`` database"""

from appeer.jobs.action import Action

class CommitAction(Action, action_type='commit'): #pylint:disable=too-many-instance-attributes
    """
    Commit parsed metadata to the ``pubs.db`` database

    A commit action attempts to insert metadata corresponding to a single
        parse action to the ``pubs.db`` database.

    The ``pubs.db`` database may contain one and only one entry for a given
        DOI. Therefore, if a commit action attempts to insert an entry
        with a DOI that already exists in ``pubs.db``, one of the following
        may happen:

            (1) The entry is rejected; i.e. the commit action does nothing.
                    This is the default behavior.

            (2) The entry is overwritten.
                    This behavior is activated if the ``overwrite`` parameter
                    of self.run() is set to True.

    Instances of the CommitAction class have dynamically created properties
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
    parse_label : str
        Label of the parse job that the metadata corresponds to
    parse_action_index : int
        Index of the parse action that the input file corresponds to
    date : str
        Date and time of the job creation/run; mutable
    doi : str
        DOI of the publication
    publisher : str
        Publisher
    journal : str
        Publication journal
    title : str
        Title of the publication
    publication_type : str
        Type of publication
    affiliations : list of str
        Author affiliations
    received : str
        Date of the publication reception
    accepted : str
        Date of the publication acceptance
    published : str
        Date of publication
    success : str
        Whether the action executed successfully; one of ('T', 'F');
            note that success will be 'T' even if the entry is
            cleanly rejected
    status : str
        Action status; one of ('I', 'W', 'R', 'E', 'X'); mutable
    passed : str
        Whether the metadata was entered into ``pubs.db``;
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
            commit_entry,
            label=None,
            action_index=None):
        """
        Creates a new commit action for a given commit entry

        Parameters
        ----------
        commit_entry : appeer.commit.commit_packer._CommitEntry
            Named tuple with fields
                ('scrape_label', 'scrape_action_index', 'metadata')
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
                parse_label=commit_entry.parse_label,
                parse_action_index=commit_entry.parse_action_index,
                status='W',
                **commit_entry.metadata)
