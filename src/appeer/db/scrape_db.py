import click
from collections import namedtuple

from appeer.db.db import DB

class ScrapeDB(DB):
    """
    Handles the ``scrape.db`` database.

    """

    def __init__(self):
        """
        If the scrape database exists, establishes a connection and a cursor.

        """

        super().__init__(db_type='scrape')

    def _define_scrape_job_tuple(self):
        """
        Defines the ``self._ScrapeJob`` named tuple, whose attributes have the same name as the columns in the ``scrape_jobs`` table.

        """

        self._ScrapeJob = namedtuple('ScrapeJob',
                """label,
                description,
                log,
                download_directory,
                zip_file,
                date,
                job_status,
                job_successes,
                job_fails,
                no_of_publications,
                job_parsed""")

    def _set_scrape_job_factory(self):
        """
        Make rows returned by the cursor be ``self._ScrapeJob`` instances.

        """

        self._define_scrape_job_tuple()

        def scrape_job_factory(cursor, row):
            return self._ScrapeJob(*row)

        self._con.row_factory = scrape_job_factory
        self._cur = self._con.cursor()

    def _initialize_database(self):
        """
        Initializes the SQL tables when the ``scrape`` database is created.

        """

        self._cur.execute('CREATE TABLE scrape_jobs(label, description, log, download_directory, zip_file, date, job_status, job_successes, job_fails, no_of_publications, job_parsed)')

        self._cur.execute('CREATE TABLE scrape(label, scrape_index, url, success_status, scrape_file, parsed)')

        self._con.commit()

    def _add_scrape_job(self,
            label,
            description,
            log,
            download_directory,
            zip_file,
            date):
        """
        Initializes an entry for a scrape job.

        Parameters
        ----------
        label : str
            Label of the scrape job
        description : str
            Description of the scrape job
        log : str
            Path to the log of the scrape job
        download_directory : str
            Path to the directory where the data is being downloaded
        zip_file : str
            Path to the output ZIP file of the scrape job
        date : str
            Date on which the scrape job was initialized

        """

        data = ({
            'label': label,
            'description': description,
            'log': log,
            'download_directory': download_directory,
            'zip_file': zip_file,
            'date': date,
            'job_status': 'I',
            'job_successes': 0,
            'job_fails': 0,
            'no_of_publications': 0,
            'job_parsed': 'F'
            })

        self._cur.execute("""
        INSERT INTO scrape_jobs VALUES(:label, :description, :log, :download_directory, :zip_file, :date, :job_status, :job_successes, :job_fails, :no_of_publications, :job_parsed)
        """, data)

        self._con.commit()

    def _update_job_entry(self, label, column_name, new_value):
        """
        Given a ``label``, updates the corresponding ``column_name`` value with ``new_value``.

        ``column_name`` must be in ['job_status', 'job_successes', 'job_fails', 'no_of publications', 'job_parsed'].

        Parameters
        ----------

        label : str
            Label of the scrape job that is being updated
        column_name : str
            Name of the column whose value is being updated
        new_value : str | int
            New value of the given column

        """

        match column_name:

            case 'job_status':

                if new_value not in ['I', 'R', 'X', 'E']:
                    raise ValueError(f'Cannot update the scrape database. Invalid job_status={new_value} given; must be "I", "R", "X" or "E".')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET job_status=? WHERE label=?
                    """, (new_value, label))
        
                    self._con.commit()

            case 'no_of_publications':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_publications={new_value} given; must be a positive integer.')
        
                elif not (new_value > 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_publications={new_value} given; must be a positive integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET no_of_publications=? WHERE label=?
                    """, (new_value, label))
        
                    self._con.commit()

            case 'job_successes':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_success={new_value} given; must be an integer.')
        
                elif not (new_value >= 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_success={new_value} given; must be a non-negative integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET job_successes=? WHERE label=?
                    """, (new_value, label))
        
                    self._con.commit()

            case 'job_fails':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_fails={new_value} given; must be an integer.')
        
                elif not (new_value >= 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_fails={new_value} given; must be a non-negative integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET job_fails=? WHERE label=?
                    """, (new_value, label))
        
                    self._con.commit()

            case 'job_parsed':

                if new_value not in ['T', 'F']:
                    raise ValueError(f'Cannot update the scrape database. Invalid job_parsed={new_value} given; must be "T" or "F".')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET job_parsed=? WHERE label=?
                    """, (new_value, label))
        
                    self._con.commit()

            case _:

                raise ValueError(f'Cannot update the scrape database. Invalid column name {column_name} given.')

    def _get_jobs(self):
        """
        Stores the data from the ``scrape_jobs`` table to the ``self.scrape_jobs`` list. 
        
        Each element of the list is a named tuple with attributes with the same name as the ``scrape_jobs`` column names.

        """

        self._set_scrape_job_factory()

        self.scrape_jobs = []

        self._cur.execute("""
        SELECT * FROM scrape_jobs
        """)

        self.scrape_jobs = self._cur.fetchall()

    def print_jobs(self):
        """
        Prints a summary of all entries in the ``scrape_jobs`` table.

        """
        
        self._get_jobs()

        header = '{:30s} {:35s} {:^4s} {:^4s} {:^10s}'.format('Label', 'Description', 'S', 'P', 'Succ./Tot.')
        header_length = len(header)
        dashes = header_length * '–'

        click.echo(dashes)
        click.echo(header)
        click.echo(dashes)

        for i, job in enumerate(self.scrape_jobs):

            description = job.description

            if len(description) > 30:
                description = description[0:30] + '...'

            succ_tot = f'{job.job_successes}/{job.no_of_publications}'

            report = '{:30s} {:35s} {:^4s} {:^4s} {:^10s}'.format(job.label, description, job.job_status, job.job_parsed, succ_tot)

            click.echo(report)

        click.echo(dashes)

        click.echo('S = Scrape job status: (I) Initialized; (R) Running; (X) Executed/Finished; (E) Error')
        click.echo('P = Scrape job completely parsed: (T) True; (F) False')
        click.echo('Succ./Tot. = Ratio of successful scrapes over total inputted URLs')

        click.echo(dashes)

    def _get_job(self, scrape_label):
        """
        Returns an instance of the ``self._ScrapeJob`` named tuple for a scrape job with label ``scrape_label``. 
        
        Parameters
        ----------
        scrape_label | str
            Label of the sought scrape job

        """

        self._set_scrape_job_factory()

        self._cur.execute("""
        SELECT * FROM scrape_jobs WHERE label = ?
        """, (scrape_label,))

        scrape_job = self._cur.fetchone()

        return scrape_job

    def _scrape_job_exists(self, scrape_label):
        """
        Checks whether the scrape job with ``scrape_label`` exists in the database.

        Parameters
        ----------
        scrape_label | str
            Label of the scrape job whose existence is being checked

        Returns
        -------
        job_exists | bool
            True if scrape job exists, False if it does not

        """

        job = self._get_job(scrape_label=scrape_label)

        if job is None:
            job_exists = False

        else:
            job_exists = True

        return job_exists

    def delete_job_entry(self, scrape_label):
        """
        Deletes the row given by ``scrape_label`` from the ``scrape_jobs`` table.

        Parameters
        ----------
        scrape_label | str
            Label of the scrape job whose entry is being removed

        Returns
        -------
        success | bool
            True if the entry was removed, False if it was not

        """

        click.echo(f'Removing entry {scrape_label} from the scrape database')
        click.echo(self._dashes)

        job_exists = self._scrape_job_exists(scrape_label)

        if not job_exists:
            click.echo(f'The entry for scrape job {scrape_label} does not exist.')
            success = False

        else:
            
            self._cur.execute("""
            DELETE FROM scrape_jobs WHERE label = ?
            """, (scrape_label,))

            self._con.commit()

            job_exists = self._scrape_job_exists(scrape_label)

            if not job_exists:
                click.echo(f'Entry {scrape_label} removed.')
                click.echo(self._dashes)
                success = True

            else:
                click.echo(f'Could not delete entry {scrape_label}')
                click.echo(self._dashes)
                success = False

        return success
