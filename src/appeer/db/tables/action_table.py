"""Defines methods common to action tables in ``jobs.db``"""

import click

from appeer.db.tables.table import Table

class ActionTable(Table,
        name=None,
        columns=None):
    """
    Defines methods common to all action tables

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

    def delete_entry(self, **kwargs):
        """
        Deletes an entry from the action table with a
            given ``label`` and ``action_index``

        Keyword Arguments
        -----------------
        label : str
            Label of the scrape job that the scrape belongs to
        action_index : int
            Index of the action to be deleted

        Returns
        -------
        success : bool
            True if entry was removed, False if it was not

        """

        label = kwargs['label']
        action_index = kwargs['action_index']

        click.echo(f'Removing action {action_index} from job {label} ...')

        exists = self.action_exists(label, action_index)

        if not exists:
            click.echo(f'The entry for action {action_index} in job {label} does not exist.')
            success = False

        else:

            self._sanity_check()

            delete_query = f'DELETE FROM {self._name} where (label = ?) AND (action_index = ?)' #pylint:disable=line-too-long

            self._cur.execute(delete_query, (label, action_index))

            self._con.commit()

            if not self.action_exists(label, action_index):

                click.echo(f'Action {action_index} removed from job {label}')
                success = True

            else:

                click.echo(f'Could not delete action {action_index} in job {label}')
                success = False

        return success

    def action_exists(self, label, action_index):
        """
        Checks whether the action with ``label`` and ``action_index``
            exists in the table

        Parameters
        ----------
        label : str
            Label of the job that the action belongs to
        action_index : int
            Index of the action

        Returns
        -------
        exists : bool
            True if action exists, False if it does not

        """

        action = self.get_action(label=label, action_index=action_index)

        exists = bool(action)

        return exists

    def get_action(self, label, action_index):
        """
        Returns an action with the given ``label`` and ``action_index``
        
        Parameters
        ----------
        label : str
            Label of the job that the action belongs to
        action_index : int
            Index of the action

        Returns
        -------
        action : appeer.db.tables.{self._name}s._{self._name}
            The sought action

        """

        action = self._search_table(label=label, action_index=action_index)

        if action:
            action = action[0]

        return action

    def get_actions_by_label(self, label):
        """
        Returns a list of actions for a job with the given ``label``
        
        Parameters
        ----------
        label : str
            Label of the job that the action belongs to

        Returns
        -------
        actions : list of appeer.db.tables.{self._name}s._{self._name}
            The sought action

        """

        actions = self._search_table(label=label)

        return actions
