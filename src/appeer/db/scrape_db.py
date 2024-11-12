import click
from collections import namedtuple

import appeer.utils as appeer_log

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

    def _initialize_database(self):
        """
        Initializes the SQL tables when the ``scrape`` database is created.

        """

        self._cur.execute('CREATE TABLE scrape_jobs(scrape_label, scrape_description, scrape_log, scrape_zip, scrape_date, scrape_job_status, scrape_job_successes, scrape_job_fails, scrape_no_of_publications, scrape_job_parsed)')
        self._cur.execute('CREATE TABLE scrape(scrape_label, scrape_index, scrape_url, scrape_success_status, scrape_file, scrape_parsed)')
        self._con.commit()

    def _add_scrape_job(self,
            scrape_label,
            scrape_description,
            scrape_log,
            scrape_zip,
            scrape_date):
        """
        Initializes an entry for a scrape job.

        Parameters
        ----------
        scrape_label : str
            Label of the scrape job
        scrape_description : str
            Description of the scrape job
        scrape_log : str
            Path to the log of the scrape job
        scrape_zip : str
            Path to the output ZIP file of the scrape job
        scrape_date : str
            Date on which the scrape job was initialized

        """

        data = ({
            'scrape_label': scrape_label,
            'scrape_description': scrape_description,
            'scrape_log': scrape_log,
            'scrape_zip': scrape_zip,
            'scrape_date': scrape_date,
            'scrape_job_status': 'I',
            'scrape_job_successes': 0,
            'scrape_job_fails': 0,
            'scrape_no_of_publications': 0,
            'scrape_job_parsed': 'F'
            })

        self._cur.execute("""
        INSERT INTO scrape_jobs VALUES(:scrape_label, :scrape_description, :scrape_log, :scrape_zip, :scrape_date, :scrape_job_status, :scrape_job_successes, :scrape_job_fails, :scrape_no_of_publications, :scrape_job_parsed)
        """, data)

        self._con.commit()

    def _update_job_entry(self, scrape_label, column_name, new_value):
        """
        Given a ``scrape_label``, updates the corresponding ``column_name`` value with ``new_value``.

        ``column_name`` must be in ['scrape_job_status', 'scrape_job_successes', 'scrape_job_fails', 'scrape_no_of publications', 'scrape_job_parsed'].

        Parameters
        ----------

        scrape_label : str
            Label of the scrape job that is being updated
        column_name : str
            Name of the column whose value is being updated
        new_value : str | int
            New value of the given column

        """

        match column_name:

            case 'scrape_job_status':

                if new_value not in ['I', 'R', 'X', 'E']:
                    raise ValueError(f'Cannot update the scrape database. Invalid scrape_job_status={new_value} given; must be "I", "R", "X" or "E".')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET scrape_job_status=? WHERE scrape_label=?
                    """, (new_value, scrape_label))
        
                    self._con.commit()

            case 'scrape_no_of_publications':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_publications={new_value} given; must be a positive integer.')
        
                elif not (new_value > 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_publications={new_value} given; must be a positive integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET scrape_no_of_publications=? WHERE scrape_label=?
                    """, (new_value, scrape_label))
        
                    self._con.commit()

            case 'scrape_job_successes':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_success={new_value} given; must be an integer.')
        
                elif not (new_value >= 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_success={new_value} given; must be a non-negative integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET scrape_job_successes=? WHERE scrape_label=?
                    """, (new_value, scrape_label))
        
                    self._con.commit()

            case 'scrape_job_fails':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_fails={new_value} given; must be an integer.')
        
                elif not (new_value >= 0):
                    raise ValueError(f'Cannot update the scrape database. Invalid no_of_fails={new_value} given; must be a non-negative integer.')

                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET scrape_job_fails=? WHERE scrape_label=?
                    """, (new_value, scrape_label))
        
                    self._con.commit()

            case 'scrape_job_parsed':

                if new_value not in ['T', 'F']:
                    raise ValueError(f'Cannot update the scrape database. Invalid scrape_job_parsed={new_value} given; must be "T" or "F".')
        
                else:
        
                    self._cur.execute("""
                    UPDATE scrape_jobs SET scrape_job_parsed=? WHERE scrape_label=?
                    """, (new_value, scrape_label))
        
                    self._con.commit()

            case _:

                raise ValueError(f'Cannot update the scrape database. Invalid column name {column_name} given.')

    def _get_scrape_jobs(self):
        """
        Stores the data from the ``scrape_jobs`` table to the ``self.scrape_jobs`` list. 
        
        Each element of the list is a named tuple with attributes with the same name as the ``scrape_jobs`` column names.

        """

        self.scrape_jobs = []

        ScrapeJob = namedtuple('ScrapeJob',
                """scrape_label,
                scrape_description,
                scrape_log,
                scrape_zip,
                scrape_date,
                scrape_job_status,
                scrape_job_successes,
                scrape_job_fails,
                scrape_no_of_publications,
                scrape_job_parsed""")

        self._cur.execute("""
        SELECT scrape_label, scrape_description, scrape_log, scrape_zip, scrape_date, scrape_job_status, scrape_job_successes, scrape_job_fails, scrape_no_of_publications, scrape_job_parsed FROM scrape_jobs
        """)

        for sj in map(ScrapeJob._make, self._cur.fetchall()):
            self.scrape_jobs.append(sj)

    def print_scrape_jobs(self):
        """
        Prints a summary of all entries in the ``scrape_jobs`` table.

        """
        
        self._get_scrape_jobs()

        header = '{:30s} {:35s} {:^4s} {:^4s} {:^10s}'.format('Label', 'Description', 'S', 'P', 'Succ./Tot.')
        header_length = len(header)
        dashes = header_length * '–'

        click.echo(dashes)
        click.echo(header)
        click.echo(dashes)

        for i, scrape_job in enumerate(self.scrape_jobs):

            description = scrape_job.scrape_description

            if len(description) > 30:
                description = description[0:30] + '...'

            succ_tot = f'{scrape_job.scrape_job_successes}/{scrape_job.scrape_no_of_publications}'

            report = '{:30s} {:35s} {:^4s} {:^4s} {:^10s}'.format(scrape_job.scrape_label, description, scrape_job.scrape_job_status, scrape_job.scrape_job_parsed, succ_tot)

            click.echo(report)

        click.echo(dashes)

        click.echo('S = Scrape job status: (I) Initialized; (R) Running; (X) Executed/Finished; (E) Error')
        click.echo('P = Scrape job completely parsed: (T) True; (F) False')
        click.echo('Succ./Tot. = Ratio of successful scrapes over total inputted URLs')

        click.echo(dashes)
