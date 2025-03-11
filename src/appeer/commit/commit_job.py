"""Commit publications to the pubs.db database"""

from appeer.jobs.job import Job


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
