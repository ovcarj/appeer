import click

from collections import namedtuple

import appeer.log

from appeer.db.db import DB

class ParseDB(DB):
    """
    Handles the ``parse.db`` database.

    """

    def __init__(self):
        """
        If the parse database exists, establishes a connection and a cursor.

        """

        super().__init__(db_type='parse')

    def _define_parse_tuple(self):
        """
        Defines the ``self._Parse`` named tuple, whose attributes have the same name as the columns in the ``parse`` table.

        """

        self._Parse = namedtuple('Parse',
                """doi,
                received,
                published,
                duration,
                publisher,
                journal,
                title,
                affiliations""")

    def _set_parse_factory(self):
        """
        Make rows returned by the cursor be ``self._Parse`` instances.

        """

        self._define_parse_tuple()

        def parse_factory(cursor, row):
            return self._Parse(*row)

        self._con.row_factory = parse_factory
        self._cur = self._con.cursor()

    def _initialize_database(self):
        """
        Initializes the SQL table when the ``parse`` database is created.

        """

        self._cur.execute('CREATE TABLE parse(doi, received, published, duration, publisher, journal, title, affiliations)')

        self._con.commit()
