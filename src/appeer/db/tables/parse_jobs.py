"""Handles the ``parse_jobs`` table in ``jobs.db``"""

import click

from appeer.db.tables.job_table import JobTable
from appeer.db.tables.registered_tables import get_registered_tables

class ParseJobs(JobTable,
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

    def add_entry(self, **kwargs):
        """
        Initializes an entry for a parse job

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
        Updates an entry in the ``parse_jobs`` table

        Given a ``label``, updates the corresponding ``column_name``
            with ``new_value`` in the ``parse_jobs`` table

        ``column_name`` must be in
            ('job_status', 'job_step', 'job_successes', 'job_fails',
            'no_of_publications', 'job_parsed')

        Keyword Arguments
        -----------------
        label : str
            Label of the parse job that is being updated
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
                    raise ValueError(f'Cannot update the parse database. Invalid job_status={new_value} given; must be "I", "R", "X" or "E".')

                self._cur.execute("""
                UPDATE parse_jobs SET job_status = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'no_of_publications':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the parse database. Invalid no_of_publications={new_value} given; must be a positive integer.')

                if not new_value > 0:
                    raise ValueError(f'Cannot update the parse database. Invalid no_of_publications={new_value} given; must be a positive integer.')

                self._cur.execute("""
                UPDATE parse_jobs SET no_of_publications = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'job_step':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the parse database. Invalid job_step={new_value} given; must be an integer.')

                if not new_value >= 0:
                    raise ValueError(f'Cannot update the parse database. Invalid job_step={new_value} given; must be a non-negative integer.')

                self._cur.execute("""
                UPDATE parse_jobs SET job_step = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'job_successes':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the parse database. Invalid no_of_success={new_value} given; must be an integer.')

                if not new_value >= 0:
                    raise ValueError(f'Cannot update the parse database. Invalid no_of_success={new_value} given; must be a non-negative integer.')

                self._cur.execute("""
                UPDATE parse_jobs SET job_successes = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'job_fails':

                if not isinstance(new_value, int):
                    raise ValueError(f'Cannot update the parse database. Invalid no_of_fails={new_value} given; must be an integer.')

                if not new_value >= 0:
                    raise ValueError(f'Cannot update the parse database. Invalid no_of_fails={new_value} given; must be a non-negative integer.')

                self._cur.execute("""
                UPDATE parse_jobs SET job_fails = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case 'job_committed':

                if new_value not in ('T', 'F'):
                    raise ValueError(f'Cannot update the parse database. Invalid job_committed={new_value} given; must be "T" or "F".')

                self._cur.execute("""
                UPDATE parse_jobs SET job_committed = ? WHERE label = ?
                """, (new_value, label))

                self._con.commit()

            case _:

                self._con.close()
                raise ValueError(f'Cannot update the parse database. Invalid column name "{column_name}" given.')

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

        click.echo('M = Parse job mode: (A) Auto; (E) Everything; (S) Parse job; (F) File list')
        click.echo('S = Parse job status: (I) Initialized; (R) Running; (X) Executed/Finished; (E) Error')
        click.echo('C = Parse job committed: (T) True; (F) False')
        click.echo('Succ./Tot. = Ratio of successful parsed publications over total inputted publications')

        click.echo(dashes)
