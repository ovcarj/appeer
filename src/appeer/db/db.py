import sys
import click
import abc
import sqlite3

import appeer.log 
import appeer.utils

from appeer.datadir import Datadir

class DB(abc.ABC):
    """
    Base abstract class for handling ``appeer`` databases,
    which are found at ``Datadir().base/db``.

    """

    def __init__(self, db_type):
        """
        If the database exists, establishes a connection and a cursor.

        Parameters
        ----------

        db_type : str
            Must be 'scrape' or 'parse'.

        """

        datadir = Datadir()

        self._base = datadir.base

        if db_type not in ['scrape', 'parse']:
            raise ValueError('Failed to initialize the DB class. db_type must be "scrape" or "parse".')

        else:
            self._db_type = db_type

        if self._db_type == 'scrape':
            self._db_path = f'{self._base}/db/scrape.db'

        elif self._db_type == 'parse':
            self._db_path = f'{self._base}/db/parse.db'

        self._check_existence()

        if self._db_exists:

            self._con = sqlite3.connect(self._db_path)
            self._cur = self._con.cursor()

        self._dashes = appeer.log.get_log_dashes()

    def _check_existence(self):
        """
        Checks the existence of the ``appeer`` database;
        the ``self._db_exists`` attribute is updated accordingly

        """

        self._db_exists = appeer.utils.file_exists(self._db_path)

    def create_database(self):
        """
        Creates the database.

        """

        self._check_existence()

        if self._db_exists:

            click.echo(f'WARNING: {self._db_type} database already exists at {self._db_path}')
            click.echo(self._dashes)
            self._handle_database_exists()

        else:

            try:
                self._con = sqlite3.connect(self._db_path)
                self._cur = self._con.cursor()

            except:
                click.echo(f'Failed to initialize the {self._db_type} database at {self._db_path}. Do you have the required permissions to write to the requested directory? Exiting.')
                sys.exit()

            self._check_existence()

            if self._db_exists:

                self._initialize_database()

                click.echo(f'{self._db_type} database initialized at {self._db_path}')

            else:
                click.echo(f'Failed to initialize the {self._db_type} database at {self._db_path}. Exiting.')
                sys.exit()

    @abc.abstractmethod
    def _initialize_database(self):
        """
        Initializes the SQL tables when the database is created.

        """
        pass

    def _handle_database_exists(self):
        """
        Handles the case when the user tries to run ``self.create_database()`` with a preexisting database.

        """
        
        input_ok = False

        proceed = appeer.log.ask_yes_no(f'Do you want to proceed with the current {self._db_type} database? [Y/n]\n')

        if proceed == 'Y':

            click.echo(f'Proceeding with the current {self._db_type} database.')

        elif proceed == 'n':

            click.echo(f'Stopping, as requested.')
            click.echo(self._dashes)

            sys.exit()
