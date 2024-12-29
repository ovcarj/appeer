"""Handles the ``parse_jobs`` table in ``jobs.db``"""

import click

from appeer.db.tables.table import Table
from appeer.db.tables.registered_tables import get_registered_tables

class ParseJobs(Table,
              name='parse_jobs',
              columns=get_registered_tables()['parse_jobs']):
    """
    Handles the ``parse_jobs`` table

    Parameters
    ----------
    name : str
        Table name
    columns : str
        List of column names

    """

    def __init__(self, connection):
        """
        Establishes a connection with the the jobs database

        Parameters
        ----------
        connection : sqlite3.Connection
            Connection to the database to which the table belongs to

        """

        super().__init__(connection=connection)

    @property
    def bad_jobs(self):
        """
        Returns all parse jobs whose status is not 'X'.

        Returns
        -------
        jobs : list
            List of ParseJob instances for which the job status is not 'X'

        """

        jobs = self._search_table(contains=[False],
                                  job_status='X')

        return jobs

    def add_entry(self, **kwargs):
        """
        Initializes an entry for a parse job.

        Keyword Arguments
        -----------------
        label : str
            Label of the parse job
        date : str
            Date on which the parse job was initialized
        description : str
            Description of the parse job
        log : str
            Path to the log of the parse job
        parse_directory : str
            Path to the directory where the (temporary) files for
                parsing are created
        mode : str
            Parsing mode. Must be in ('A', 'E', 'S', 'F')

        """

        if kwargs['mode'] not in ('A', 'E', 'S', 'F'):
            raise ValueError(f'Cannot add entry to the parse jobs database; invalid mode {kwargs["mode"]} inputted.')

        data = ({
            'label': kwargs['label'],
            'date': kwargs['date'],
            'description': kwargs['description'],
            'log': kwargs['log_path'],
            'parse_directory': kwargs['parse_directory'],
            'mode': kwargs['mode'],
            'job_status': 'I',
            'job_step': 0,
            'job_successes': 0,
            'job_fails': 0,
            'no_of_publications': 0,
            'job_committed': 'F'
            })

        self._cur.execute("""
        INSERT INTO parse_jobs VALUES(:label, :date, :description, :log, :parse_directory, :mode, :job_status, :job_step, :job_successes, :job_fails, :no_of_publications, :job_committed)
        """, data)

        self._con.commit()

    def update_entry(self, **kwargs):
        """
        Updates an entry

        """

    def delete_entry(self, **kwargs):
        """
        Deletes the row given by ``label`` from the ``parse_jobs`` table, 
            along with all corresponding entries in the ``parses`` table

        Keyword Arguments
        -----------------
        label : str
            Label of the parse job whose entry is being removed

        Returns
        -------
        success : bool
            True if the entry was removed, False if it was not

        """

        label = kwargs['label']

        click.echo(f'Removing entry {label} from the parse database ...')

        exists = self.job_exists(label)

        if not exists:

            click.echo(f'The entry for parse job {label} does not exist.')
            success = False

        else:

            self._cur.execute("""
            DELETE FROM parse_jobs WHERE label = ?
            """, (label,))

            self._con.commit()

            exists = self.job_exists(label)

            if not exists:

                self._cur.execute("""
                DELETE FROM parses WHERE label = ?
                """, (label,))

                self._con.commit()

                click.echo(f'Entry {label} removed.\n')
                success = True

            else:
                click.echo(f'Could not delete entry {label}\n')
                success = False

        return success

    def job_exists(self, label):
        """
        Checks whether the job with the given label exists in the database

        Parameters
        ----------
        label : str
            Label of the parse job whose existence is being checked

        Returns
        -------
        exists : bool
            True if parse job exists, False if it does not

        """

        exists = bool(self.get_job(label=label))

        return exists

    def get_job(self, label):
        """
        Returns an instance of the ``self._ParseJob`` named tuple
        for a parse job with the given ``label``
        
        Parameters
        ----------
        label : str
            Label of the sought parse job

        Returns
        -------
        job : appeer.db.tables.parse_jobs._ParseJobs
            The sought parse job

        """

        job = self._search_table(label=label)

        if job:
            job = job[0]

        return job

    def print_summary(self):
        """
        Prints a summary of the entries in the ``parse_jobs`` table

        """

        header = '{:30s} {:25s} {:^4s} {:^4s} {:^4s} {:^10s}'.format('Label', 'Description', 'M', 'S', 'C', 'Succ./Tot.')
        header_length = len(header)
        dashes = header_length * '–'

        click.echo(dashes)
        click.echo(header)
        click.echo(dashes)

        for job in self.entries:

            description = job.description

            if len(description) > 20:
                description = description[0:20] + '...'

            succ_tot = f'{job.job_successes}/{job.no_of_publications}'

            report = '{:30s} {:25s} {:^4s} {:^4s} {:^4s} {:^10s}'.format(job.label, description, job.mode, job.job_status, job.job_committed, succ_tot)

            click.echo(report)

        click.echo(dashes)

        click.echo('M = Parse job mode: (A) Auto; (E) Everything; (S) Scrape job; (F) File list')
        click.echo('S = Parse job status: (I) Initialized; (R) Running; (X) Executed/Finished; (E) Error')
        click.echo('C = Parse job committed: (T) True; (F) False')
        click.echo('Succ./Tot. = Ratio of successful parsed publications over total inputted publications')

        click.echo(dashes)
