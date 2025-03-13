"""Base abstract class for handling appeer databases"""

import os
import sys
import abc
import importlib
import sqlite3

from functools import partial

import click

from appeer.general import log
from appeer.general import utils
from appeer.general.datadir import Datadir

from appeer.db.tables.registered_tables import get_registered_tables

class DB(abc.ABC):
    """
    Base abstract class for handling ``appeer`` databases,
        which are found at ``Datadir().base/db``

    """

    def __init_subclass__(cls, tables):
        """
        Attributes the given tables to the database class

        Parameters
        ----------
        tables : list
            List of tables belonging to the database

        """

        if not isinstance(tables, list):
            raise TypeError('Tables must be given as a list.')

        registered_tables = get_registered_tables().keys()

        for table in tables:

            if not isinstance(table, str):
                raise TypeError('Table name must be a string.')

            if table not in registered_tables:
                raise PermissionError(f'Unknown table {table} given. Allowed table names: {list(registered_tables)}')

        cls._table_classes = {}
        cls.tables = tables

        for table in tables:

            _table_module = importlib.import_module(
                    f'appeer.db.tables.{table}')
            _table_class_name = "".join([word.capitalize()
                for word in table.split('_')])

            _table_class = getattr(_table_module, _table_class_name)

            cls._table_classes[f'{table}'] = _table_class

    def __init__(self, db_type):
        """
        If the database exists, establishes a connection and a cursor.

        Parameters
        ----------
        db_type : str
            Must be 'jobs' or 'pub'.

        """

        datadir = Datadir()

        self._base = datadir.base

        if db_type not in ['jobs', 'pub']:
            raise ValueError('Failed to initialize the DB class. db_type must be "jobs" or "pub".')

        self._db_type = db_type

        if self._db_type == 'jobs':
            self._db_path = os.path.join(datadir.db, 'jobs.db')

        elif self._db_type == 'pub':
            self._db_path = os.path.join(datadir.db, 'pub.db')

        if self._db_exists:

            self._con = sqlite3.connect(self._db_path)
            self._cur = self._con.cursor()

            def _get_table_instance(self, connection, tab_class): #pylint:disable=unused-argument
                return tab_class(connection)

            for table_name, table_class in self._table_classes.items():
                setattr(self.__class__,
                        table_name,
                        property(partial(_get_table_instance,
                                         connection=self._con,
                                         tab_class=table_class))
                        )

        self._dashes = log.get_log_dashes()

    @property
    def _db_exists(self):
        """
        Checks for the existence of a database at self._db_path

        Returns
        -------
        _db_exists : bool
            True if a file exists at self._db_path, False otherwise

        """

        exists = utils.file_exists(self._db_path)

        return exists

    @_db_exists.setter
    def _db_exists(self, value):
        """
        This attribute should never be set directly

        """

        raise PermissionError('The "_db_exists" attribute cannot be directly set')

    def create_database(self):
        """
        Creates the database.

        """

        if self._db_exists:

            click.echo(f'WARNING: {self._db_type} database already exists at {self._db_path}')
            click.echo(self._dashes)
            self._handle_database_exists()

        else:

            try:
                self._con = sqlite3.connect(self._db_path)
                self._cur = self._con.cursor()

            except PermissionError:
                click.echo(f'Failed to initialize the {self._db_type} database at {self._db_path}. Do you have the required permissions to write to the requested directory? Exiting.')
                sys.exit()

            if self._db_exists:

                self.__init__() #pylint:disable=unnecessary-dunder-call

                self._initialize_database()

                click.echo(f'{self._db_type} database initialized at {self._db_path}')

            else:
                click.echo(f'Failed to initialize the {self._db_type} database at {self._db_path}. Exiting.')
                sys.exit()

    def _handle_database_exists(self):
        """
        Handles the case when the user tries to run ``self.create_database()``
            with a preexisting database.

        """

        proceed = log.ask_yes_no(f'Do you want to proceed with the current {self._db_type} database? [Y/n]\n')

        if proceed == 'Y':

            click.echo(f'Proceeding with the current {self._db_type} database.')

        elif proceed == 'n':

            click.echo('Stopping, as requested.')
            click.echo(self._dashes)

            sys.exit()

    def _initialize_database(self):
        """
        Initializes the SQL tables when the database is created.

        """

        for table in self.tables:
            getattr(self, table).initialize_table()
