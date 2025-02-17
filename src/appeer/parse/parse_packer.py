"""Prepare the inputted data for parsing"""

import os
from collections import namedtuple

import click

import appeer.parse.parse_reports as reports

from appeer.general import utils as _utils

_ParseEntry = namedtuple('_ParseEntry', [
    'scrape_label',
    'scrape_action_index',
    'filepath'
    ])

class ParsePacker:
    """
    Prepare the inputted data for parsing

    Used by ParseJob to check the validity of the inputted data
        and prepare the data in a unified format.

    """

    def __init__(self, parse_mode, data_source, parse_directory, _queue=None):
        """
        Checks if the ``parse_mode`` and ``data_source`` are in valid format

        Parameters
        ----------
        parse_mode : str
            Parsing mode; one of ('A', 'E', 'S', 'F')
        data_source : None | list of str
            None for parse modes ('A', 'E'),
                list of scrape job labels ('S'),
                list of file paths ('F')
        parse_directory : str
            Directory into which to (temporarily) create files for parsing
        _queue : queue.Queue
            If given, messages will be logged in the job log file

        """

        self.success = False
        self._queue = _queue

        os.makedirs(parse_directory, exist_ok=True)

        match parse_mode:

            case 'A' | 'E':
                if data_source is not None:
                    raise ValueError(f'Failed to initialize ParsePacker; the data_source must be None in parse_mode="{parse_mode}".\n')

            case 'S' | 'F':
                if not _utils.is_list_of_str(data_source):
                    raise ValueError(f'Failed to initialize ParsePacker; in parse_mode="{parse_mode}", the data_source must be provided as a list of strings or a single string in parse_mode="{parse_mode}".\n')

            case _:
                raise ValueError(f'Failed to initialize ParsePacker; invalid parse_mode={parse_mode} inputted.\n')

        if not os.access(parse_directory, os.W_OK):
            raise ValueError(f'Failed to initialize ParsePacker; the directory {parse_directory} is not writeable.\n')

        self._parse_mode = parse_mode
        self._data_source = data_source

        self.report = ''

        self.packet = []

    def pack(self):
        """
        Runs the appropriate packing method depending on the parse mode

        """

        match self._parse_mode:

            case 'A':
                raise NotImplementedError('Parse mode "A" not yet implemented')

            case 'E':
                raise NotImplementedError('Parse mode "E" not yet implemented')

            case 'S':
                raise NotImplementedError('Parse mode "S" not yet implemented')

            case 'F':
                self._pack_file_list()

    def _pack_file_list(self):
        """
        Checks a list of filepaths for readability and existence

        In this case, ``self._data_source`` must be a list of strings
            or a single string

        """

        file_list = self._data_source

        if isinstance(file_list, str):
            file_list = [file_list]

        no_of_publications = len(file_list)

        self._pprint(f'\nInputted {no_of_publications} files.')
        self._pprint('Checking if files exist and are readable...\n')

        files_readability = _utils.file_list_readable(file_list=file_list)
        self._pprint(reports.files_readability_report(files_readability))

        valid_files = [f for f, readable in files_readability.items()
                if readable]

        if valid_files:

            self._pprint(f'Determined that {len(valid_files)}/{no_of_publications} file(s) are readable.')

            valid_entries = [_ParseEntry(
                scrape_label=None,
                scrape_action_index=None,
                filepath=valid_file)
                for valid_file in valid_files]

            self.packet.extend(valid_entries)

            self.success = True

        else:

            self._pprint('None of the inputted files are readable.')
            self.success = False

    def _pprint(self, message):
        """
        Prints a ``message`` to stdout or puts it in the queue
        
        If the message is put into the queue, it will be logged in
            the job log file

        Parameters
        ----------
        message : str
            String to be printed to stdout or logged in the job log file

        """

        if self._queue:
            self._queue.put(message)

        else:
            click.echo(message)
