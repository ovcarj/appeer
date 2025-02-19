"""Prepare (scraped) files for parsing"""

import os
from collections import namedtuple

import click

import appeer.parse.parse_reports as reports

from appeer.general import utils as _utils

from appeer.scrape import scrape_scripts
from appeer.scrape import scrape_reports
from appeer.scrape.scrape_job import ScrapeJob


_ParseEntry = namedtuple('_ParseEntry', [
    'scrape_label',
    'scrape_action_index',
    'filepath'
    ])

class ParsePacker:
    """
    Prepare (scraped) files for parsing

    Used by ParseJob to check the validity of the inputted data
        and prepare the files in a unified format.

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
        self._parse_directory = parse_directory

        self.packet = []

    def pack(self):
        """
        Runs the appropriate packing method depending on the parse mode

        """

        match self._parse_mode:

            case 'A':
                self._pack_mode_A()

            case 'E':
                raise NotImplementedError('Parse mode "E" not yet implemented')

            case 'S':
                self._pack_mode_S()

            case 'F':
                self._pack_mode_F()

    def _pack_mode_F(self):
        """
        Prepares a parse packet from a list of filepaths

        ``self._data_source`` must be a list of strings or a single string.

        Only files that exist and are readable are added to the packet

        """

        file_list = self._data_source

        if isinstance(file_list, str):
            file_list = [file_list]

        no_of_publications = len(file_list)

        self._pprint(f'\nInputted {no_of_publications} file(s).')
        self._pprint('Checking if files exist and are readable...\n')

        files_readability = _utils.file_list_readable(file_list=file_list)
        self._pprint(reports.files_readability_report(files_readability))

        valid_files = [f for f, readable in files_readability.items()
                if readable]

        if valid_files:

            self._pprint(f'Determined that {len(valid_files)}/{no_of_publications} files are readable.')

            valid_entries = [_ParseEntry(
                scrape_label=None,
                scrape_action_index=None,
                filepath=valid_file)
                for valid_file in valid_files]

            self.packet.extend(valid_entries)

            self.success = True

        else:
            self._pprint('None of the inputted files are readable.')

    def _pack_mode_A(self):
        """
        Prepares a parse packet automatically from unparsed scrape jobs

        ``self._data_source`` will be automatically set.

        The packet is prepared only for jobs in the executed ('X') status.

        In mode 'A', only previously unparsed actions are taken into account.

        """

        self._pprint('Searching for unparsed scrape jobs...')

        unparsed_job_labels = scrape_scripts.get_unparsed_job_labels()

        if unparsed_job_labels:

            self._pprint(f'Found {len(unparsed_job_labels)} jobs.')
            self._pprint('Preparing the data for parsing...')
            self._data_source = unparsed_job_labels

            self._pack_mode_S()

        else:
            self._pprint('No unparsed jobs found.')

    def _pack_mode_S(self):
        """
        Prepares a parse packet from a list of scrape job labels

        ``self._data_source`` must be a list of strings or a single string.

        The packet is prepared only for jobs in the executed ('X') status.

            For those jobs, ``self._prepare_scrape_actions`` is invoked
            to search for successful actions.

            If needed, the scrape job output ZIP files are extracted to the 
            temporary parsing directory

        """

        scrape_labels = self._data_source

        if isinstance(scrape_labels, str):
            scrape_labels = [scrape_labels]

        no_of_jobs = len(scrape_labels)

        self._pprint(f'\nInputted {no_of_jobs} scrape job label(s).')
        self._pprint('Checking if the jobs exist and if their status is executed (X)...\n')

        scrape_jobs_execution_dict = scrape_scripts.get_execution_dict(
                job_labels=scrape_labels)

        self._pprint(scrape_reports.scrape_jobs_execution_report(
            scrape_jobs_execution_dict=scrape_jobs_execution_dict))

        valid_jobs = [job for job, execution_status in
                scrape_jobs_execution_dict.items()
                if execution_status == 'X']

        if valid_jobs:

            self._pprint(f'Determined that {len(valid_jobs)}/{no_of_jobs} inputted scrape jobs are executed.\n')

            self._prepare_scrape_files(scrape_labels=valid_jobs)

        else:
            self._pprint('None of the inputted scrape jobs exist or are executed.\n')

    # TODO: This method is too long and does too many different things
    def _prepare_scrape_files(self, scrape_labels): #pylint:disable=too-many-locals, too-many-branches
        """
        Prepare files for parsing from a list of scrape job labels

        If the files are available (probably no cleanup was done after a scrape
            job), no unzipping is done. If the files are not available, the
            output ZIP file of the scrape job is unzipped to the temporary
            parse directory.

        The scrape jobs are searched for successful actions.

        In parse mode 'A', if a scrape action was already parsed,
            it is ignored.
        In modes 'E' and 'S', the scrape actions will be considered 
            regardless if they were previously parsed.

        This method should *never* be invoked directly, but only through the
            ``self._pack_*`` methods.

        If a scrape job with an execution status other than 'X' is passed
            in ``scrape_labels``, an error is raised

        Parameters
        ----------
        scrape_labels : list of str
            List of scrape job labels

        """

        match self._parse_mode:

            case 'A':
                ignore_parsed_actions = True

            case 'E' | 'S':
                ignore_parsed_actions = False

            case _:
                raise ValueError('ParsePacker failed; _prepare_scrape_files was invoked in invalid parse mode.')

        relevant_actions = []

        if ignore_parsed_actions:
            self._pprint('Searching for successful and unparsed scrape actions...')

        else:
            self._pprint('Searching for successful scrape actions...')

        for scrape_label in scrape_labels:

            scrape_job = ScrapeJob(label=scrape_label)

            if not scrape_job._job_exists: #pylint:disable=protected-access
                raise ValueError('ParsePacker failed; nonexistent scrape job inputted.')
            if not scrape_job.job_status == 'X':
                raise ValueError('ParsePacker failed; inputted a scrape job which was not executed to _prepare_scrape_files.')

            successful_actions = scrape_job.successful_actions

            if successful_actions and ignore_parsed_actions:

                relevant_actions.extend(
                        [action for action in successful_actions
                        if action.parsed == 'F'])

            else:
                relevant_actions.extend(successful_actions)

        if relevant_actions:

            no_of_publications = len(relevant_actions)
            self._pprint(f'Found {no_of_publications} scrape actions.\n')

            self._pprint('Checking the scraped files for readability and existence...\n')

            action_files = scrape_scripts.check_action_outputs(
                    actions=relevant_actions)

            self._pprint(scrape_reports.scrape_output_files_report(
                action_files=action_files))

            files_accessible = [action_file.file_ok
                    for action_file in action_files]

            if all(accessible is True for accessible in files_accessible):
                self._pprint(f'All found files ({no_of_publications}/{no_of_publications}) are directly accessible (no ZIP file extraction needed).\n')

                self.packet.extend([_ParseEntry(
                    scrape_label=action_file.label,
                    scrape_action=action_file.action_index,
                    filepath=action_file.out_file)
                    for action_file in action_files
                        ])

                self.success = True

            else:

                accessible_files = [action_file for action_file in action_files
                        if action_file.file_ok is True]

                inaccessible_files = [action_file for action_file
                        in action_files if action_file.file_ok is False]

                self._pprint(f'Found {len(accessible_files)}/{no_of_publications} directly accessible files.\n')

                self.packet.extend([_ParseEntry(
                    scrape_label=action_file.label,
                    scrape_action_index=action_file.action_index,
                    filepath=action_file.out_file)
                    for action_file in accessible_files
                        ])

                unique_scrape_job_labels = list(set(inaccessible_file.label
                    for inaccessible_file in inaccessible_files))

                self._pprint(f'The remaining {len(inaccessible_files)}/{no_of_publications} files belong to {len(unique_scrape_job_labels)} scrape job(s).')
                self._pprint('Extracting the corresponding scrape job ZIP files...\n')

                for scrape_job_label in unique_scrape_job_labels:

                    sj = ScrapeJob(scrape_job_label)

                    target_directory = os.path.join(
                            self._parse_directory, scrape_job_label)

                    extract_success = _utils.extract_archive(
                            zip_filename=sj.zip_file,
                            target_directory=target_directory)

                    if extract_success:

                        self._pprint(f'Extracted {sj.zip_file} to {target_directory}')

                        #
                        # Lines below look ugly, but they are only mapping
                        # the scrape actions to the just extracted files
                        #
                        # The _ParseEntry corresponding to the scrape action
                        # is added to self.packet if the newly
                        # extracted file is readable
                        #
                        self.packet.extend([_ParseEntry(
                            scrape_label=scrape_job_label,
                            scrape_action_index=action_file.action_index,
                            filepath=os.path.join(target_directory,
                                os.path.basename(action_file.out_file)))

                            for action_file in inaccessible_files

                            if (action_file.label == scrape_job_label and
                                _utils.file_list_readable(
                                    os.path.join(target_directory,
                                    os.path.basename(action_file.out_file)))
                                    )
                            ])

                    else:
                        self._pprint(f'Failed to extract {sj.zip_file}')

                self.success = True

        else:
            self._pprint('No valid actions were found.')

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
