"""Base abstract class for handling tables in sqlite3 databases"""

import os
import sys
import abc
import sqlite3
import click

from collections import namedtuple

from appeer.general import log
from appeer.general import utils

from appeer.general.datadir import Datadir

class Table(abc.ABC):
    """
    Base abstract class for handling tables in the sqlite3 databases

    """

    def __init_subclass__(cls, name, columns):
        """
        Ensures that every Table subclass defines ``name`` and ``columns``

        Parameters
        ----------
        name : str
            Name of the table in the database
        columns : list
            List of column names

        """

        if not isinstance(name, str):
            raise TypeError('Table name must be a string.')

        if not isinstance(columns, list):
            raise TypeError(f'Columns of table {name} must be given as a list of strings.')

        for column in columns:
            if not isinstance(column, str):
                raise TypeError(f'Non-string value given for a column in {columns} in table {name}.')

        cls.name = name
        cls.columns = columns

    def __init__(self, connection):
        """
        Defines the row factory and convenient queries for the table

        connection : sqlite3.Connection
            Connection to the database to which the table belongs to

        """

        self._con = connection

        table_columns = namedtuple(f'_{self.name}', self.columns)

        def row_factory(cursor, row):
            return table_columns(*row)

        self._con.row_factory = row_factory
        self._cur = self._con.cursor()

        self._columns_string = ', '.join(self.columns)

        self.initialize_query = f'CREATE TABLE {self.name}({self._columns_string})'
        self.general_query = f'SELECT * FROM {self.name}'
