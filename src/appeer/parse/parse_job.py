"""Parse scraped publications"""

import os
import threading
import queue

from appeer.general.datadir import Datadir
from appeer.general import log as _log
from appeer.general import utils as _utils

from appeer.jobs.job import Job

from appeer.scrape.scrape_job import ScrapeJob

from appeer.parse.parse_action import ParseAction
from appeer.parse.parse_packer import ParsePacker
from appeer.parse import parse_reports as reports


class ParseJob(Job, job_type='parse_job'): #pylint:disable=too-many-instance-attributes
    """
    Parse scraped publications

    The ``mode`` attribute defines four available parsing modes:

        1) Auto (A) [DEFAULT]
            Parse files found through the ``scrape_jobs`` table,
            for the scrape jobs that were executed but not previously parsed.
            Note that only the scrape actions which were not previously parsed
            will be parsed. Default mode

        2) Everything (E) 
            Parse all files found through the ``scrape_jobs`` table, regardless
            of whether the jobs and the corresponding actions were previously
            parsed. Only executed jobs are considered

        3) Scrape job (S) 
            Parse a list of scrape jobs with the given labels, regardless of 
            whether the jobs and the corresponding actions were previously
            parsed

        4) File list (F) 
            Parse a list of files. This mode is independent of previous
            scrape jobs

    Instances of the ParseJob class have dynamically created properties
        corresponding to the columns of the ``parse_jobs`` table in the
        ``jobs`` database

        Some of these properties are mutable; changing their value will
        update the value in the ``jobs`` database. The values of the
        non-mutable properties are set using ``self.new_job(**kwargs)``.
        The values of these properties may be edited only if
        ``self._job_mode == 'write'

    List of the dynamically generated properties:

    Dynamic Properties
    ------------------
    date : str
        Job creation date and time
    description : str
        Optional job description
    log : str
        Path to the job log file
    parse_directory : str
        Directory into which to (temporarily) create files for parsing
    mode : str
        Parsing mode; one of ('A', 'E', 'S', 'F'); 'A' is default
    job_status : str
        Job status; one of ('I', 'W', 'R', 'E', 'X'); mutable
    job_step : int
        Index of the current job action; mutable
    job_successes : int
        Counts how many actions were succesfully ran; mutable
    job_fails : int
        Counts how many actions failed; mutable
    no_of_publications : int
        Counts how many publications were added to the job; mutable
    job_committed : str
        Whether all parsed data corresponding to the job is committed;
            one of ('T', 'F'); mutable

    """

    def __init__(self,
            label=None,
            job_mode='read'):
        """
        Connects to the job database and sets the job label

        Parameters
        ----------
        label : str
            Unique job label
        job_mode : str
            Must be 'read' or 'write'

        """

        super().__init__(label=label, job_mode=job_mode)

    def _prepare_new_job_parameters(self, **kwargs):
        """
        Helper method to prepare parse parameters given to ``self.new_job()``

        Keyword arguments are the same as the ones in ``self.new_job()``,
            excluding ``label``

        Keyword Arguments
        -----------------
        description : str
            Optional job description
        log_directory : str
            Directory into which to store the log file
        parse_directory : str
            Directory into which to (temporarily) create files for parsing
        mode : str
            Parsing mode; one of ('A', 'E', 'S', 'F'); 'A' is default

        Returns
        -------
        job_arguments : dict
            The inputted keyword arguments with added defaults

        """

        datadir = Datadir()

        default_parse_directory = os.path.join(datadir.parse,
                self.label)
        kwargs.setdefault('parse_directory', default_parse_directory)

        if kwargs['parse_directory'] is None:
            kwargs['parse_directory'] = default_parse_directory

        kwargs['parse_directory'] = os.path.abspath(
                kwargs['parse_directory'])

        kwargs.setdefault('mode', 'A')

        kwargs.setdefault('description', None)
        kwargs.setdefault('log_directory', None)

        job_parameters = {
                'description': kwargs['description'],
                'parse_directory': kwargs['parse_directory'],
                'log_directory': kwargs['log_directory'],
                'mode': kwargs['mode']
                }

        return job_parameters

    def add_publications(self, data_source=None):
        """
        Add publications to be parsed

        Depending on the ``self.mode`` attribute, data_source may be:

            1) (A): None
            2) (E): None
            3) (S): List of scrape job labels
            4) (F): List of file paths

        Parameters
        ----------
        data_source : None | list of str
            None for modes ('A', 'E'), list of scrape job labels ('S'),
                list of file paths ('F')

        """

        if not self._job_exists:
            raise PermissionError('Cannot add publications to a parse job; most likely, the job has not yet been initialized.')

        if self.job_status == 'X':
            raise PermissionError(f'Cannot add new publications to the parse job "{self.label}"; the job has already been executed.')

        if self.job_status == 'R':
            raise PermissionError(f'Cannot add new publications to the parse job "{self.label}"; the job is in the "R" (Running) state.')

        if self.job_status == 'E':
            raise PermissionError(f'Cannot add new publications to the parse job "{self.label}"; the job is in the "E" (Error) state.')

        self._job_mode = 'write'

        self._queue = queue.Queue()
        threading.Thread(target=self._log_server, daemon=True).start()

        self._prepare_parsing(data_source=data_source)

        self._queue.join()

    def _prepare_parsing(self, data_source):
        """
        Prepare inputted data for parsing depending on the parsing mode

        Parameters
        ----------
        data_source : None | list of str
            None for modes ('A', 'E'), list of scrape job labels ('S'),
                list of file paths ('F')

        """

        self._wlog(_log.boxed_message('PREPARING PARSING', centered=True) + '\n')

        try:

            packer = ParsePacker(parse_mode=self.mode,
                    data_source=data_source,
                    parse_directory=self.parse_directory,
                    _queue=self._queue)

        except ValueError as err:
            self._wlog(err)

        else:

            packer.pack()

            if packer.success:
                self._queue.put(
                        f'\nPrepared {len(packer.packet)} files for parsing.')

                self._add_actions(parse_packet=packer.packet)

                self.no_of_publications = len(self.actions)
                self.job_status = 'W'

            else:
                self._queue.put('No new actions were added to the parse job.')

    def _add_actions(self, parse_packet):
        """
        Add ParseActions to the ParseJob

        ``parse_packet`` is given by ``appeer.parse.parse_packer.packet``

        Parameters
        ----------
        parse_packet : list of appeer.parse.parse_packer._ParseEntry
            Packet containing info on the filepaths to be parsed
                (and possibly scrape labels and action indices from which
                the files originate)

        """

        for i, parse_entry in enumerate(parse_packet):

            action = ParseAction(label=self.label,
                    action_index=self.no_of_publications + i)

            action.new_action(parse_entry=parse_entry)

    def _prepare_run_parameters(self, restart_mode, cleanup, **kwargs):
        """
        Helper method to prepare run arguments for ``self.run_job()``

        Parameters and keyword arguments are the same as in self.run_job()

        """

        if restart_mode not in ('from_scratch', 'resume'):
            raise ValueError('scrape_mode must be "from_scratch" or "resume".')

        if not isinstance(cleanup, bool):
            raise ValueError('The "cleanup" parameter must be boolean.')

        kwargs.setdefault('publishers', None)
        kwargs.setdefault('journals', None)
        kwargs.setdefault('data_types', 'txt')

        kwargs.setdefault('no_scrape_mark', False)

        no_scrape_mark = kwargs['no_scrape_mark']

        if not isinstance(no_scrape_mark, bool):
            raise ValueError('The "no_scrape_mark" parameter must be boolean.')

        publishers, journals, data_types =\
                kwargs['publishers'],\
                kwargs['journals'],\
                kwargs['data_types']

        if publishers and not _utils.is_list_of_str(publishers):
            raise TypeError('Invalid "publishers" argument passed to run_job; must be a string or a list of strings.')

        if journals and not _utils.is_list_of_str(journals):
            raise TypeError('Invalid "journals" argument passed to run_job; must be a string or a list of strings.')

        if (data_types and data_types not in ('txt', ['txt'])):
            raise NotImplementedError('Only "txt" data type is currently implemented.')

        run_parameters = {'restart_mode': restart_mode,
                'no_scrape_mark': no_scrape_mark,
                'cleanup': cleanup,
                'publishers': publishers,
                'journals': journals,
                'data_types': data_types,
                }

        return run_parameters

    def run_action(self, action_index=None, _queue=None, **action_parameters):
        """
        Runs the parse action given by ``action_index``

        Parameters
        ----------
        action_index : int
            Index of the parse action to run; defaults to
                ``self.job_step``
        _queue : queue.Queue
            If given, messages will be logged in the job log file

        Keyword Arguments
        -----------------
        no_scrape_mark : bool
            If True, scrape jobs will not be labeled as parsed
                even if they are parsed successfully
        publishers : str | list of str | None
            List of candidate parser publisher codes
        journals : str | list of str | None
            List of candidate parser journal codes
        data_types : str | list of str | None
            List of candidate parser data types;
                currently, only 'txt' is supported

        """

        if not action_index:
            action_index = self.job_step

        self._wlog(reports.parse_step_report(self,
            action_index=action_index))

        self.actions[action_index].run(
                _queue=self._queue,
                **action_parameters)

        if self.actions[action_index].success == 'F':
            self.job_fails += 1

        if self.actions[action_index].success == 'T':

            self.job_successes += 1

    def _update_scrape(self, action_index):
        """
        Updates the "parsed" status of a scrape action corresponding to the
            parse action defined by ``action_index``

        The status is updated only if the parse action is successful.

        If the parse action does not correspond to a scrape action
            (parse mode "F"), does nothing.

        If all the successful scrape actions within a scrape job
            are parsed, the scrape job will also be marked as
            completely parsed

        """

        parse_action = self.actions[action_index]

        if (parse_action.scrape_label is not None and\
                parse_action.scrape_action_index is not None and
                parse_action.success == 'T'):

            scrape_job = ScrapeJob(label=parse_action.scrape_label,
                    job_mode='write')

            scrape_action = scrape_job.actions\
                    [parse_action.scrape_action_index]

            scrape_action.mark_as_parsed()

            scrape_job.update_parsed()
