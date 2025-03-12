"""Prepare parsed data for committing"""

from collections import namedtuple

from appeer.general import utils as _utils


_CommitEntry = namedtuple('_CommitEntry', [
    'parse_label',
    'parse_action_index',
    'metadata'
    ])

class CommitPacker:
    """
    Prepare parsed data for committing

    Used by CommitJob to check the validity of the inputted data
        and prepare the data in a unified format

    """

    def __init__(self, commit_mode, data_source, _queue=None):
        """
        Checks if the ``parse_mode`` and ``data_source`` are in valid format

        Parameters
        ----------
        commit_mode : str
            Commit mode; one of ('A', 'E', 'P')
        data_source : None | list of str
            None for commit modes ('A', 'E'),
                list of parse job labels ('P'),
        _queue : queue.Queue
            If given, messages will be logged in the job log file

        """

        self.success = False
        self._queue = _queue

        match commit_mode:

            case 'A' | 'E':
                if data_source is not None:
                    raise ValueError(f'Failed to initialize CommitPacker; the data_source must be None in commit_mode="{commit_mode}".\n')

            case 'P':
                if not _utils.is_list_of_str(data_source):
                    raise ValueError(f'Failed to initialize CommitPacker; in commit_mode="{commit_mode}", the data_source must be provided as a list of strings or a single string."\n')

            case _:
                raise ValueError(f'Failed to initialize CommitPacker; invalid commit_mode={commit_mode} inputted.\n')

        self._commit_mode = commit_mode
        self._data_source = data_source

        self.packet = []

    def pack(self):
        """
        Runs the appropriate packing method depending on the commit mode

        """

        match self._commit_mode:

            case 'A':
                raise NotImplementedError('CommitPacker: mode "A" not implemented.')

            case 'E':
                raise NotImplementedError('CommitPacker: mode "E" not implemented.')

            case 'P':
                raise NotImplementedError('CommitPacker: mode "P" not implemented.')
