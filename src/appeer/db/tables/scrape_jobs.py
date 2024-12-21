"""Handles the ``scrape_jobs`` table in ``jobs.db``"""

import click

from appeer.general import log
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

        connection : sqlite3.Connection
            Connection to the database to which the table belongs to

        """

        super().__init__(connection=connection)

    @property
    def bad_jobs(self):
        """
        Returns all scrape jobs whose status is not 'X'.

        Returns
        -------
        jobs : list
            List of ScrapeJob instances for which the job status is not 'X'

        """

        jobs = self._search_table(contains=[False],
                                  job_status='X')

        return jobs

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
        scrapes : list
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

    def print_summary(self):
        """
        Prints a summary of all entries in the ``scrape_jobs`` table

        """

        header = '{:30s} {:35s} {:^4s} {:^4s} {:^10s}'.format('Label', 'Description', 'S', 'P', 'Succ./Tot.')
        header_length = len(header)
        dashes = header_length * '–'

        click.echo(dashes)
        click.echo(header)
        click.echo(dashes)

        for job in self.entries:

            description = job.description

            if len(description) > 30:
                description = description[0:30] + '...'

            succ_tot = f'{job.job_successes}/{job.no_of_publications}'

            report = '{:30s} {:35s} {:^4s} {:^4s} {:^10s}'.format(job.label, description, job.job_status, job.job_parsed, succ_tot)

            click.echo(report)

        click.echo(dashes)

        click.echo('S = Scrape job status: (I) Initialized; (W) Waiting; (R) Running; (X) Executed/Finished; (E) Error')
        click.echo('P = Scrape job completely parsed: (T) True; (F) False')
        click.echo('Succ./Tot. = Ratio of successful scrapes over total inputted URLs')

        click.echo(dashes)

    def print_job_details(self, label):
        """
        Prints details of the job with the given label.

        Parameters
        ----------
        label : str
            Label of the job for which the details are printed

        """

        exists = self.job_exists(label)

        if not exists:

            click.echo(f'Scrape job {label} does not exist.')

        else:

            dashes = log.get_log_dashes()

            job = self.get_job(label)

            sgr = reports.scrape_general_report(job=job)

            click.echo(sgr)

            click.echo('{:19s} {:s}'.format('Job status', job.job_status))
            click.echo('{:19s} {:d}/{:d}'.format('Succ./Tot.', job.job_successes, job.no_of_publications))
            click.echo('{:19s} {:s}'.format('Completely parsed', job.job_parsed))
            click.echo(dashes)

            scrapes = self.get_scrapes(label)

            if not scrapes:
                click.echo('No files downloaded')

            else:

                click.echo(log.boxed_message('SCRAPE DETAILS'))

                header = '{:<10s} {:<10s} {:^4s} {:^8s} {:<16s} {:<80s}'.format('Index', 'Strategy', 'S', 'P', 'Output', 'URL')
                dashes_details = len(header) * '–'

                click.echo(dashes_details)
                click.echo(header)
                click.echo(dashes_details)

                for scrape in scrapes:

                    click.echo('{:<10d} {:<10s} {:^4s} {:^8s} {:<16s} {:<80s}'.format(scrape.action_index, scrape.strategy, scrape.status, scrape.parsed, scrape.out_file, scrape.url))

                click.echo(dashes_details)

                click.echo('S = Scrape status: (I) Initialized; (W) Waiting; (R) Running; (X) Executed/Finished; (E) Error')
                click.echo('P = Scrape parsed: (T) True; (F) False')
                click.echo(dashes_details)
