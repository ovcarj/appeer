import click

from collections import namedtuple

import appeer.log

from appeer.db.db import DB

class PubDB(DB):
    """
    Handles the ``pub.db`` database.

    """

    def __init__(self):
        """
        If the pub database exists, establishes a connection and a cursor.

        """

        super().__init__(db_type='pub')

    def _define_pub_tuple(self):
        """
        Defines the ``self._Pub`` named tuple, whose attributes have the same name as the columns in the ``pub` table.

        """

        self._Pub = namedtuple('Pub',
                """doi,
                received,
                published,
                duration,
                publisher,
                journal,
                title,
                affiliations""")

    def _set_pub_factory(self):
        """
        Make rows returned by the cursor be ``self._Pub`` instances.

        """

        self._define_pub_tuple()

        def pub_factory(cursor, row):
            return self._Pub(*row)

        self._con.row_factory = pub_factory
        self._cur = self._con.cursor()

    def _initialize_database(self):
        """
        Initializes the SQL table when the ``pub`` database is created.

        """

        self._cur.execute('CREATE TABLE pub(doi, received, published, duration, publisher, journal, title, affiliations)')

        self._con.commit()
