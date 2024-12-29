"""Handles the ``scrape_jobs`` table in ``jobs.db``"""

import click

from appeer.scrape import reports

from appeer.db.tables.table import Table
from appeer.db.tables.scrapes import Scrapes
from appeer.db.tables.registered_tables import get_registered_tables

class ScrapeJobs(Table,
                 name='scrape_jobs',
                 columns=get_registered_tables()['scrape_jobs']):
    """
    Handles the ``scrape_jobs`` table

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
        Returns all scrape jobs whose status is not 'X'

        Returns
        -------
        jobs : list
            List of ScrapeJob instances for which the job status is not 'X'

        """

        jobs = self._search_table(contains=[False],
                                  job_status='X')

        return jobs

    @property
    def summary(self):
        """
        Get a formatted summary of all scrape jobs in the table

        Returns
        -------
        _summary : str
            Formatted summary of all scrape jobs in the table

        """

        _summary = reports.scrape_jobs_summary(self)

        return _summary

    def add_entry(self, **kwargs):
        """
        Initializes an entry in the scrape_jobs table

        Keyword Arguments
        -----------------
        label : str
            Label of the scrape job
        description : str
            Description of the scrape job
        log_path : str
            Path to the log of the scrape job
        download_directory : str
            Path to the directory where the data is being downloaded
        zip_file : str
            Path to the output ZIP file of the scrape job
        date : str
            Date on which the scrape job was initialized

        """

        data = ({
            'label': kwargs['label'],
            'description': kwargs['description'],
            'log': kwargs['log_path'],
            'download_directory': kwargs['download_directory'],
            'zip_file': kwargs['zip_file'],
            'date': kwargs['date'],
            'job_status': 'I',
            'job_step': 0,
            'job_successes': 0,
            'job_fails': 0,
            'no_of_publications': 0,
            'job_parsed': 'F'
            })

        self._sanity_check()

        add_query = """
        INSERT INTO scrape_jobs VALUES(:label, :description, :log, :download_directory, :zip_file, :date, :job_status, :job_step, :job_successes, :job_fails, :no_of_publications, :job_parsed)
        """

        self._cur.execute(add_query, data)

        self._con.commit()

    def update_entry(self, **kwargs):
        """
        Updates an entry in the scrape_jobs table

        Given a ``label``, updates the corresponding ``column_name``
            with ``new_value`` in the ``scrape_jobs`` table

        ``column_name`` must be in
            ('job_status', 'job_successes', 'job_fails',
            'no_of publications', 'job_parsed')

        Keyword Arguments
        -----------------
        label : str
            Label of the scrape job that is being updated
        column_name : str
            Name of the column whose value is being updated
        new_value : str : int
            New value of the given column

        """

        self._sanity_check()

        label = kwargs['label']
        column_name = kwargs['column_name']
        new_value = kwargs['new_value']

        match column_name:

            case 'job_status':

                if new_value not in ('I', 'W', 'R', 'X', 'E'):
                    raise ValueError(f'Cannot update the scrape database. Invalid job_status={new_value} given; must be "I", "R", "X" or "E".')

                self._cur.execute("""
                UPDATE scrape_jobs SET job_status = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'no_of_publications':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_publications={new_value} given; must be a positive integer.')

                if not new_value > 0:
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_publications={new_value} given; must be a positive integer.')

                self._cur.execute("""
                UPDATE scrape_jobs SET no_of_publications = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'job_step':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid job_step={new_value} given; must be an integer.')

                if not new_value >= 0:
                    raise ValueError(f'Cannot update the scrape database. Invalid job_step={new_value} given; must be a non-negative integer.')

                self._cur.execute("""
                UPDATE scrape_jobs SET job_step = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'job_successes':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_success={new_value} given; must be an integer.')

                if not new_value >= 0:
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_success={new_value} given; must be a non-negative integer.')

                self._cur.execute("""
                UPDATE scrape_jobs SET job_successes = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'job_fails':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_fails={new_value} given; must be an integer.')

                if not new_value >= 0:
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_fails={new_value} given; must be a non-negative integer.')

                self._cur.execute("""
                UPDATE scrape_jobs SET job_fails = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'job_parsed':

                if new_value not in ('T', 'F'):
                    raise ValueError(f'Cannot update the scrape database. Invalid job_parsed={new_value} given; must be "T" or "F".')

                self._cur.execute("""
                UPDATE scrape_jobs SET job_parsed = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case _:

                self._con.close()
                raise ValueError(f'Cannot update the scrape database. Invalid column name "{column_name}" given.')


    def delete_entry(self, **kwargs):
        """
        Deletes the row given by ``label`` from the ``scrape_jobs`` table, 
        along with all corresponding entries in the ``scrapes`` table

        Keyword Arguments
        -----------------
        label : str
            Label of the scrape job whose entry is being removed

        Returns
        -------
        success : bool
            True if the entry was removed, False if it was not

        """

        label = kwargs['label']

        click.echo(f'Removing entry {label} from the scrape database ...')

        exists = self.job_exists(label)

        if not exists:

            click.echo(f'The entry for scrape job {label} does not exist.')
            success = False

        else:

            self._cur.execute("""
            DELETE FROM scrape_jobs WHERE label = ?
            """, (label,))

            self._con.commit()

            exists = self.job_exists(label)

            if not exists:

                self._cur.execute("""
                DELETE FROM scrapes WHERE label = ?
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
        Checks whether the scrape job with ``label`` exists in the table

        Parameters
        ----------
        label : str
            Label of the scrape job whose existence is being checked

        Returns
        -------
        exists : bool
            True if scrape job exists, False if it does not

        """

        exists = bool(self.get_job(label=label))

        return exists

    def get_job(self, label):
        """
        Returns an instance of the ``self._ScrapeJob`` named tuple
        for a scrape job with the given ``label``
        
        Parameters
        ----------
        label : str
            Label of the sought scrape job

        Returns
        -------
        job : appeer.db.tables.scrape_jobs._ScrapeJobs
            The sought scrape job

        """

        job = self._search_table(label=label)

        if job:
            job = job[0]

        return job

    def get_scrapes(self, label):
        """
        Returns all scrapes for a given job label

        Parameters
        ----------
        label : str
            Label of the job for which the scrapes are returned

        Returns
        -------
        scrapes_label : list
            List of Scrape instances for the given label

        """

        exists = self.job_exists(label)

        if not exists:

            click.echo(f'Scrape job {label} does not exist.')
            scrapes_label = []

        else:

            scrapes = Scrapes(connection=self._con)
            scrapes_label = scrapes.get_scrapes_by_label(label)

        return scrapes_label
