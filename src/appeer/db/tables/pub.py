"""Handles the ``pub`` table in ``pub.db``"""

import sqlite3

import click

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

    def initialize_table(self):
        """
        Initializes an empty table

        This method overrides the ``initialize_table()`` method
            of the parent Table class. The reason is that we
            want to explicitly define ``doi`` as the primary table
            key.

        Possibly, in the future, all tables could be redefined
            with an explicit primary key.

        """

        self._sanity_check()

        columns_commas = ', '.join(self._columns)
        initialize_query = f'CREATE TABLE {self._name}({columns_commas}, PRIMARY KEY(doi COLLATE NOCASE))' #pylint:disable=line-too-long

        self._cur.execute(initialize_query)
        self._con.commit()

    def add_entry(self, overwrite=False, **kwargs):
        """
        Adds or replaces an entry in the ``pub`` table

        Each entry in the ``pub`` table must have a unique ``doi`` value.

            If an entry containing a DOI value already existing in the table
                is attempted to be added to the database, the ``overwrite``
                parameter governs the behavior of this method:

                (1) If ``overwrite == False``, the entry is not inserted

                (2) If ``overwrite == True``, the entry with the given
                    DOI is updated

        Parameters
        ----------
        overwrite : bool
            If False, ignore a duplicate DOI entry (default);
                if True, overwrite a duplicate DOI entry;
                if the given DOI is unique, this parameter has no impact

        Keyword Arguments
        -----------------
        doi : str
            DOI of the publication
        publisher : str
            Publisher
        journal : str
            Publication journal
        title : str
            Title of the publication
        publication_type : str
            Type of publication
        affiliations : str
            Author affiliations
        received : str
            Date of the publication reception
        accepted : str
            Date of the publication acceptance
        published : str
            Date of publication

        Returns
        -------
        duplicate : bool
            Whether the entry DOI is a duplicate
        inserted : bool
            Whether the entry was inserted into the table

        """

        duplicate = False
        inserted = False

        try:

            add_query = """
            INSERT INTO pub VALUES(:doi, :publisher, :journal, :title, :publication_type, :affiliations, :received, :accepted, :published)
            """

            self._cur.execute(add_query, kwargs)
            self._con.commit()

            inserted = True

        except sqlite3.IntegrityError:

            duplicate = True

            if overwrite:
                replace_query = """
                REPLACE INTO pub VALUES(:doi, :publisher, :journal, :title, :publication_type, :affiliations, :received, :accepted, :published)
                """

                self._cur.execute(replace_query, kwargs)
                self._con.commit()

                inserted = True

            else:
                pass

        return duplicate, inserted

    def update_entry(self, **kwargs):
        """
        Entries in the ``pub`` table should not be directly updated

        Instead, a commit job with ``overwrite=True`` should be run.

        """

        raise PermissionError('Directly updating entries in the "pub" table of the "pub.db" database is not permitted; to update an entry, run a commit job with overwrite=True instead.')

    def delete_entry(self, **kwargs):
        """
        Deletes an entry given by the ``doi`` keyword argument

        Keyword Arguments
        -----------------
        doi : str
            DOI of the publication

        Returns
        -------
        success : bool
            True if the entry was removed, False if it was not

        """

        doi = kwargs['doi']

        click.echo(f'Removing entry {doi} from the {self._name} table ...')

        exists = self.pub_exists(doi)

        if not exists:

            click.echo(f'Entry {doi} does not exist in the {self._name} table.')
            success = False

        else:

            self._sanity_check()

            delete_query = f'DELETE FROM {self._name} where doi = ?'

            self._cur.execute(delete_query, (doi,))

            self._con.commit()

            exists = self.pub_exists(doi)

            if not exists:

                click.echo(f'Entry {doi} removed.\n')
                success = True

            else:
                click.echo(f'Could not delete entry {doi}\n')
                success = False

        return success

    def pub_exists(self, doi):
        """
        Checks whether the entry with the given DOI exists in the table

        Parameters
        ----------
        doi : str
            DOI of the publication

        Returns
        -------
        exists : bool
            True if entry exists, False if it does not

        """

        exists = bool(self.get_pub(doi=doi))

        return exists

    def get_pub(self, doi):
        """
        Returns a named tuple corresponding to the entry with the given DOI

        Parameters
        ----------
        doi : str
            DOI of the publication

        Returns
        -------
        pub : appeer.db.tables.pub._pub
            The sought publication entry

        """

        pub = self._search_table(doi=doi)

        if pub:
            pub = pub[0]

        return pub
