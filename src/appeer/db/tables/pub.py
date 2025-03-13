"""Handles the ``pub`` table in ``pub.db``"""

from appeer.db.tables.table import Table
from appeer.db.tables.registered_tables import get_registered_tables

class Pub(Table,
           name='pub',
           columns=get_registered_tables()['pub']):
    """
    Handles the ``pub`` table

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

    def add_entry(self, **kwargs):
        """
        Initializes an entry

        """

    def update_entry(self, **kwargs):
        """
        Updates an entry

        """

    def delete_entry(self, **kwargs):
        """
        Deletes an entry

        """
