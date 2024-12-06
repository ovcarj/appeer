import click

from collections import namedtuple

from appeer.general import log

from appeer.db.db import DB

class PubsDB(DB, tables=['pubs']):
    """
    Handles the ``pubs.db`` database.

    """

    def __init__(self):
        """
        If the pubs database exists, establishes a connection and a cursor.

        """

        super().__init__(db_type='pubs')
