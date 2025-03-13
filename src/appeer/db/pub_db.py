"""Interface to the pub.db database"""

from appeer.db.db import DB

class PubDB(DB, tables=['pub']):
    """
    Interface to the pub.db database

    """

    def __init__(self):
        """
        If the pub database exists, establishes a connection and a cursor.

        """

        super().__init__(db_type='pub')
