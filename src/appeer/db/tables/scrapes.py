"""Handles the ``scrapes`` table in ``jobs.db``"""

import click

from appeer.db.tables.table import Table
from appeer.db.tables.registered_tables import get_registered_tables

class Scrapes(Table,
              name='scrapes',
              columns=get_registered_tables()['scrapes']):
    """
    Handles the ``scrapes`` table

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
    def unparsed(self):
        """
        Returns all unparsed scrapes (status='X', parsed='F').

        Returns
        -------
        unparsed_scrapes : list of appeer.db.tables.scrapes._Scrape
            Unparsed scrapes

        """

        unparsed_scrapes = self._search_table(status='X', parsed='F')

        return unparsed_scrapes

    def add_entry(self, **kwargs):
        """
        Initializes an entry for a scrape

        Keyword Arguments
        -----------------
        label : str
            Label of the scrape job
        scrape_index : int
            Index of the URL in the input
        url : str
            Inputted URL
        strategy : str
            Strategy used for scraping

        """

        data = ({
            'label': kwargs['label'],
            'scrape_index': kwargs['scrape_index'],
            'url': kwargs['url'],
            'strategy': kwargs['strategy'],
            'status': 'I',
            'out_file': 'no_file',
            'parsed': 'F'
            })

        self._sanity_check()

        add_query = """
        INSERT INTO scrapes VALUES(:label, :scrape_index, :url, :strategy, :status, :out_file, :parsed)
        """

        self._cur.execute(add_query, data)

        self._con.commit()

    def update_entry(self, **kwargs):
        """
        Given a ``label`` and ``scrape_index``, updates the corresponding 
        ``column_name`` value with ``new_value`` in the ``scrape`` table

        ``column_name`` must be in ['strategy', 'status', 'out_file', 'parsed']

        Keyword Arguments
        -----------------
        label : str
            Label of the job that the scrape belongs to
        scrape_index : int
            Index of the URL in the input
        column_name : str
            Name of the column whose value is being updated
        new_value : str : int
            New value of the given column

        """

        self._sanity_check()

        label = kwargs['label']
        scrape_index = kwargs['scrape_index']
        column_name = kwargs['column_name']
        new_value = kwargs['new_value']

        match column_name:

            case 'strategy':

                # TODO: strategies in the ScrapePlan will be revised
                # if False is left as a placeholder
                if False:
                    raise ValueError(f'Cannot update the scrape database. Invalid strategy={new_value} given; must be ...')

                else:

                    self._cur.execute("""
                    UPDATE scrapes SET strategy = ? WHERE label = ? AND scrape_index = ?
                    """, (new_value, label, scrape_index))

                    self._con.commit()

            case 'status':

                if not new_value in ['I', 'R', 'E', 'X']:
                    raise ValueError(f'Cannot update the scrape database. Invalid status={new_value} given; must be one of ["I", "R", "E", "X"].')

                self._cur.execute("""
                UPDATE scrapes SET status = ? WHERE label = ? AND scrape_index = ?
                """, (new_value, label, scrape_index))

                self._con.commit()

            case 'out_file':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the scrape database. Invalid out_file={new_value} given; must be a string.')

                self._cur.execute("""
                UPDATE scrapes SET out_file = ? WHERE label = ? AND scrape_index = ?
                """, (new_value, label, scrape_index))

                self._con.commit()

            case 'parsed':

                if new_value not in ['T', 'F']:
                    raise ValueError(f'Cannot update the scrape database. Invalid parsed={new_value} given; must be "T" or "F".')

                self._cur.execute("""
                UPDATE scrapes SET parsed = ? WHERE label = ? AND scrape_index = ?
                """, (new_value, label, scrape_index))

                self._con.commit()

            case _:

                raise ValueError(f'Cannot update the scrape database. Invalid column name {column_name} given.')

    def delete_entry(self, **kwargs):
        """
        Deletes an entry from the ``scrapes`` table with a
        given label and index

        Keyword Arguments
        -----------------
        label : str
            Label of the scrape job that the scrape belongs to
        index : int
            Index of the scrape

        Returns
        -------
        success : bool
            True if entry was removed, False if it was not

        """

        label = kwargs['label']
        index = kwargs['index']

        click.echo(f'Removing scrape {index} from job {label} from the scrape database ...')

        exists = self.scrape_exists(label, index)

        if not exists:
            click.echo(f'The entry for scrape {index} for job {label} does not exist.')
            success = False

        else:

            self._cur.execute("""
            DELETE FROM scrapes WHERE (label = ?) AND (index = ?)
            """, (label, index))

            self._con.commit()

            if not self.scrape_exists(label, index):

                click.echo(f'Entry {index} removed from job {label}')
                success = True

            else:

                click.echo(f'Could not delete entry {label}')
                success = False

        return success

    def scrape_exists(self, label, index):
        """
        Checks whether the scrape with ``label`` and ``index``
        exists in the table

        Parameters
        ----------
        label : str
            Label of the scrape job that scrape belongs to
        index : int
            Index of the scrape

        Returns
        -------
        exists : bool
            True if scrape exists, False if it does not

        """

        scrape = self.get_scrape(label=label, index=index)

        exists = bool(scrape)

        return exists

    def get_scrape(self, label, index):
        """
        Returns an instance of the ``self._Scrape`` named tuple
        for a scrape job with the given ``label`` and ``index``
        
        Parameters
        ----------
        label : str
            Label of the scrape job that scrape belongs to
        index : int
            Index of the scrape

        Returns
        -------
        scrape : appeer.db.tables.scrapes._Scrape
            The sought scrape

        """

        scrape = self._search_table(label=label, index=index)

        if scrape:
            scrape = scrape[0]

        return scrape

    def get_scrapes_by_label(self, label):
        """
        Returns a list of ``self._Scrape`` named tuples
        for a scrape job with the given ``label``
        
        Parameters
        ----------
        label : str
            Label of the scrape job that scrape belongs to

        Returns
        -------
        scrapes : list of appeer.db.tables.scrapes._Scrape
            The sought scrape

        """

        scrapes = self._search_table(label=label)

        return scrapes

    def print_unparsed(self):
        """
        Prints a summary of all unparsed scrapes

        """

        scrapes = self.unparsed

        if scrapes:

            header = '{:<30s} {:<6s} {:<65s}'.format('Label', 'Index', 'URL')
            dashes_details = len(header) * '–'

            click.echo(dashes_details)
            click.echo(header)
            click.echo(dashes_details)

            for scrape in scrapes:

                click.echo('{:<30s} {:<6d} {:<65s}'.format(
                    scrape.label, scrape.scrape_index, scrape.url))

            click.echo(dashes_details)

        else:
            click.echo('No unparsed scrapes found.')
