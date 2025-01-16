"""Handles the ``scrape_jobs`` table in ``jobs.db``"""

import click

from appeer.db.tables.table import Table

from appeer.db.tables.scrapes import Scrapes
from appeer.db.tables.parses import Parses

class JobTable(Table, name=None, columns=None):
    """
    Defines methods common to the job tables

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
        Returns all jobs whose status is not 'X'

        Returns
        -------
        jobs : list
            List of job instances for which the job status is not 'X'

        """

        jobs = self._search_table(contains=[False],
                                  job_status='X')

        return jobs

    def delete_entry(self, **kwargs):
        """
        Deletes a job entry

        Removes the row given by ``label`` from the ``jobs`` table, 
            along with all the entries in the corresponding ``actions`` table

        Keyword Arguments
        -----------------
        label : str
            Label of the job whose entry is being removed

        Returns
        -------
        success : bool
            True if the entry was removed, False if it was not

        """

        label = kwargs['label']

        click.echo(f'Removing entry {label} from the {self._name} table ...')

        exists = self.job_exists(label)

        if not exists:

            click.echo(f'Entry {label} does not exist in the {self._name} table.')
            success = False

        else:

            self._sanity_check()

            delete_query = f'DELETE FROM {self._name} where label = ?'

            self._cur.execute(delete_query, (label,))

            self._con.commit()

            exists = self.job_exists(label)

            if not exists:

                actions_table = self._name.split()[0] + 's'

                delete_actions = f'DELETE FROM {actions_table} WHERE label = ?'

                self._cur.execute(delete_actions, (label,))

                self._con.commit()

                click.echo(f'Entry {label} removed.\n')
                success = True

            else:
                click.echo(f'Could not delete entry {label}\n')
                success = False

        return success

    def job_exists(self, label):
        """
        Checks whether the job with ``label`` exists in the table

        Parameters
        ----------
        label : str
            Label of the job whose existence is being checked

        Returns
        -------
        exists : bool
            True if scrape job exists, False if it does not

        """

        exists = bool(self.get_job(label=label))

        return exists

    def get_job(self, label):
        """
        Returns a named tuple corresponding to the job with the given ``label``
        
        Parameters
        ----------
        label : str
            Label of the sought job

        Returns
        -------
        job : appeer.db.tables.*_jobs._*job
            The sought scrape job

        """

        job = self._search_table(label=label)

        if job:
            job = job[0]

        return job

    def get_actions(self, label):
        """
        Returns all actions for a given job label

        Parameters
        ----------
        label : str
            Label of the job for which the actions are returned

        Returns
        -------
        actions_label : list
            List of action named tuples for the given label

        """

        actions_label = []

        exists = self.job_exists(label)

        if not exists:

            click.echo(f'Job {label} does not exist.')

        else:

            match self._name:

                case 'scrape_jobs':

                    scrapes = Scrapes(connection=self._con)
                    actions_label = scrapes.get_scrapes_by_label(label)

                case 'parse_jobs':

                    parses = Parses(connection=self._con)
                    actions_label = parses.get_parses_by_label(label)

        return actions_label
