"""Handles the ``parses`` table in ``jobs.db``"""

import click

from appeer.db.tables.table import Table
from appeer.db.tables.registered_tables import get_registered_tables

from appeer.parse import reports

from appeer.general.utils import check_doi_format

class Parses(Table,
        name='parses',
        columns=get_registered_tables()['parses']):
    """
    Handles the ``parses`` table

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

    @property
    def uncommitted(self):
        """
        Returns all uncommitted parses (status='X', committed='F')

        Returns
        -------
        uncommitted_parses : list of appeer.db.tables.parses._Parse
            Unparsed parses

        """

        unparsed_parses = self._search_table(status='X', parsed='F')

        return unparsed_parses

    @property
    def uncommitted_summary(self):
        """
        Formatted summary of all uncommitted parses

        Returns
        -------
        _summary : str
            Summary of all uncommitted parses

        """

#       TODO: this report will be implemented
#        _summary = reports.uncommitted_parses(parses=self)

#        return _summary

    def add_entry(self, **kwargs):
        """
        Initializes an entry for a parse

        Keyword Arguments
        -----------------
        label : str
            Label of the parse job
        action_index : int
            Index of the URL in the input
        date : str
            Date and time of the parse
        input_file : str
            Path to the file to be parsed

        """

        data = ({
            'label': kwargs['label'],
            'action_index': kwargs['action_index'],
            'date': kwargs['date'],
            'input_file': kwargs['input_file'],
            'doi': '?',
            'publisher': '?',
            'journal': '?',
            'title': '?',
            'affiliations': '?',
            'received': '?',
            'accepted': '?',
            'published': '?',
            'parser': '?',
            'success': 'F',
            'status': 'W',
            'committed': 'F'
            })

        self._sanity_check()

        add_query = """
        INSERT INTO parses VALUES(:label, :action_index, :date, :input_file, :doi, :publisher, :journal, :title, :affiliations, :received, :accepted, :published, :parser, :success, :status, :committed)
        """

        self._cur.execute(add_query, data)

        self._con.commit()

    def update_entry(self, **kwargs):
        """
        Given a ``label`` and ``action_index``, updates the corresponding 
            ``column_name`` value with ``new_value`` in the ``parse`` table

        ``column_name`` must be in ('date',
            'doi', 'publisher', 'journal', 'title', 'affiliations',
            'received', 'accepted', 'published',
            'parser', 'success', 'status', 'committed'
            )

        Keyword Arguments
        -----------------
        label : str
            Label of the job that the parse belongs to
        action_index : int
            Index of the URL in the input
        column_name : str
            Name of the column whose value is being updated
        new_value : str : int
            New value of the given column

        """

        self._sanity_check()

        label = kwargs['label']
        action_index = kwargs['action_index']
        column_name = kwargs['column_name']
        new_value = kwargs['new_value']

        match column_name:

            case 'date':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid date={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE parses SET date = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'doi':

                if not check_doi_format(new_value):
                    raise ValueError(f'Cannot update the parse database. Invalid doi={new_value} given; must be valid DOI format')

                self._cur.execute("""
                UPDATE parses SET doi = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'publisher':

                # TODO: explicit publisher check should be implemented
                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid date={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE parses SET publisher = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'journal':

                # TODO: explicit journal check should be implemented
                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid journal={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE parses SET publisher = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'title':

                # TODO: explicit journal check should be implemented
                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid title={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE parses SET title = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'affiliations':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid affiliations={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE parses SET affiliations = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'received':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid received={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE parses SET received = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'accepted':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid accepted={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE parses SET accepted = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'published':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid published={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE parses SET published = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'parser':

                #TODO: explicit parser check should be implemented
                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the parse database. Invalid parser={new_value} given; must be ...')

                self._cur.execute("""
                UPDATE parses SET parser = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'success':

                if not new_value in ('T', 'F'):
                    raise ValueError(f'Cannot update the parse database. Invalid success={new_value} given; must be one of ("T", "F").')

                self._cur.execute("""
                UPDATE parses SET success = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'status':

                if not new_value in ('I', 'W', 'R', 'E', 'X'):
                    raise ValueError(f'Cannot update the parse database. Invalid status={new_value} given; must be one of ("I", "W", "R", "E", "X").')

                self._cur.execute("""
                UPDATE parses SET status = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'committed':

                if new_value not in ('T', 'F'):
                    raise ValueError(f'Cannot update the parse database. Invalid parsed={new_value} given; must be "T" or "F".')

                self._cur.execute("""
                UPDATE parses SET parsed = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case _:

                self._con.close()
                raise ValueError(f'Cannot update the parse database. Invalid column name "{column_name}" given.')

        self._con.close()

    def delete_entry(self, **kwargs):
        """
        Deletes an entry from the ``parses`` table with a
            given label and action_index

        Keyword Arguments
        -----------------
        label : str
            Label of the parse job that the parse belongs to
        action_index : int
            Index of the parse

        Returns
        -------
        success : bool
            True if entry was removed, False if it was not

        """

        label = kwargs['label']
        action_index = kwargs['action_index']

        click.echo(f'Removing parse {action_index} from job {label} from the parse database ...')

        exists = self.parse_exists(label, action_index)

        if not exists:
            click.echo(f'The entry for parse {action_index} for job {label} does not exist.')
            success = False

        else:

            self._cur.execute("""
            DELETE FROM parses WHERE (label = ?) AND (action_index = ?)
            """, (label, action_index))

            self._con.commit()

            if not self.parse_exists(label, action_index):

                click.echo(f'Entry {action_index} removed from job {label}')
                success = True

            else:

                click.echo(f'Could not delete entry {label}')
                success = False

        return success

    def parse_exists(self, label, action_index):
        """
        Checks whether the parse with ``label`` and ``action_index``
        exists in the table

        Parameters
        ----------
        label : str
            Label of the parse job that parse belongs to
        action_index : int
            Index of the parse

        Returns
        -------
        exists : bool
            True if parse exists, False if it does not

        """

        parse = self.get_parse(label=label, action_index=action_index)

        exists = bool(parse)

        return exists

    def get_parse(self, label, action_index):
        """
        Returns an instance of the ``self._Parse`` named tuple
        for a parse job with the given ``label`` and ``action_index``
        
        Parameters
        ----------
        label : str
            Label of the parse job that parse belongs to
        action_index : int
            Index of the parse

        Returns
        -------
        parse : appeer.db.tables.parses._Parse
            The sought parse

        """

        parse = self._search_table(label=label, action_index=action_index)

        if parse:
            parse = parse[0]

        return parse

    def get_parses_by_label(self, label):
        """
        Returns a list of ``self._Parse`` named tuples
            for a parse job with the given ``label``
        
        Parameters
        ----------
        label : str
            Label of the parse job that parse belongs to

        Returns
        -------
        parses : list of appeer.db.tables.parses._Parse
            The sought parse

        """

        parses = self._search_table(label=label)

        return parses
