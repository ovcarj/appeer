"""Commit publications to the pub.db database"""

import queue
import threading

from appeer.general import log as _log

from appeer.jobs.job import Job

from appeer.commit.commit_action import CommitAction
from appeer.commit.commit_packer import CommitPacker


class CommitJob(Job, job_type='commit_job'): #pylint:disable=too-many-instance-attributes
    """
    Commit parsed publications

    The ``mode`` attribute defines four available commit modes:

        1) Auto (A) [DEFAULT]
            Commit publications found through the ``parse_jobs`` table,
            for the parse jobs that were executed but not previously
            committed.

            Note that only the parse actions which were not previously
            committed will be committed. Default mode

        2) Everything (E)
            Commit all publications found through the ``parse_jobs`` table,
            regardless of whether the jobs and the corresponding actions were
            previously committed. Only executed jobs are considered

        3) Parse job (P)
            Commit a list of parse jobs with the given labels, regardless of 
            whether the jobs and the corresponding actions were previously
            parsed

    Instances of the CommitJob class have dynamically created properties
        corresponding to the columns of the ``commit_jobs`` table in the
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
    mode : str
        Commit mode; one of ('A', 'E', 'P'); 'A' is default
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
        Set defaults and sanitize parameters passed to ``self.new_job()``

        Keyword Arguments
        -----------------
        description : str
            Optional job description
        log_directory : str
            Directory into which to store the log file
        mode : str
            Commit mode; one of ('A', 'E', 'P'); 'A' is default

        Returns
        -------
        job_arguments : dict
            The inputted keyword arguments with added defaults

        """

        kwargs.setdefault('mode', 'A')

        kwargs.setdefault('description', None)
        kwargs.setdefault('log_directory', None)

        job_parameters = {
                'description': kwargs['description'],
                'log_directory': kwargs['log_directory'],
                'mode': kwargs['mode']
                }

        return job_parameters

    def add_publications(self, data_source=None):
        """
        Add publications to be committed

        Depending on the ``self.mode`` attribute, data_source may be:

            1) (A): None
            2) (E): None
            3) (P): List of parse job labels

        Parameters
        ----------
        data_source : None | list of str
            None for modes ('A', 'E'), list of scrape job labels ('P'),

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

        self._prepare_committing(data_source=data_source)

        self._queue.join()

    def _prepare_committing(self, data_source):
        """
        Prepare inputted data for committing depending on the commit mode

        Parameters
        ----------
        data_source : None | list of str
            None for modes ('A', 'E'), list of parse job labels ('P'),

        """

        self._wlog(_log.boxed_message('PREPARING METADATA', centered=True) + '\n')

        try:

            packer = CommitPacker(commit_mode=self.mode,
                    data_source=data_source,
                    _queue=self._queue)

        except ValueError as err:
            self._wlog(err)

        else:

            packer.pack()

            if packer.success:
                self._queue.put(
                        f'Prepared {len(packer.packet)} entries.')

                self._add_actions(commit_packet=packer.packet)

                self.no_of_publications = len(self.actions)
                self.job_status = 'W'

            else:
                self._queue.put('No new actions were added to the commit job.')

    def _add_actions(self, commit_packet):
        """
        Add CommitActions to the CommitJob

        ``commit_packet`` is given by ``appeer.commit.commit_packer.packet``

        Parameters
        ----------
        commit_packet : list of appeer.commit.commit_packer._CommitEntry
            Packet containing info on the metadata to be committed

        """

        for i, commit_entry in enumerate(commit_packet):

            action = CommitAction(label=self.label,
                    action_index=self.no_of_publications + i)

            action.new_action(commit_entry=commit_entry)
