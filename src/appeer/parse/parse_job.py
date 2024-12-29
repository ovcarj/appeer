"""Parse scraped publications"""

import os

from appeer.general.datadir import Datadir
from appeer.general.log import get_logo

from appeer.jobs.job import Job
from appeer.parse import reports


class ParseJob(Job, job_type='parse_job'): #pylint:disable=too-many-instance-attributes
    """
    Parse scraped publications

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
        Parsing mode; one of ('E', 'A', 'S', F); 'A' is default; mutable
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

        self.__queue = None

    def new_job(self, **kwargs):
        """
        Create a new parse job

        A number of options may be provided through keyword arguments (see
            below). If not provided, defaults are generated

        Keyword Arguments
        -----------------
        label : str
            Unique job label
        description : str
            Optional job description
        log_directory : str
            Directory into which to store the log file
        parse_directory : str
            Directory into which to (temporarily) create files for parsing
        mode : str
            Parsing mode; one of ('E', 'A', 'S', F); 'A' is default; mutable

        """

        self._job_mode = 'write'

        kwargs.setdefault('label', None)
        self._qualify_job_label(kwargs['label'])

        job_parameters = self.__prepare_new_job_parameters(**kwargs)

        self._initialize_job_common(**job_parameters)

        self._wlog(get_logo())
        self._wlog(reports.parse_general_report(job=self))

    def __prepare_new_job_parameters(self, **kwargs):
        """
        Helper method to prepare scrape parameters given to ``self.new_job()``

        Keyword arguments are the same as the ones in ``self.new_job()``

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

    def __log_server(self):
        """
        Log messages received from actions through self.__queue

        """

        if not self.__queue:
            raise ValueError('Cannot log action message; self.__queue has not been initialized.')

        while True:

            message = self.__queue.get()
            self._wlog(message)
            self.__queue.task_done()
