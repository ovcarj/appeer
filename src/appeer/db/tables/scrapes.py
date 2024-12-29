"""Handles the ``scrapes`` table in ``jobs.db``"""

import click

from appeer.db.tables.table import Table
from appeer.db.tables.registered_tables import get_registered_tables

from appeer.scrape import reports

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
        Returns all unparsed scrapes (status='X', parsed='F')

        Returns
        -------
        unparsed_scrapes : list of appeer.db.tables.scrapes._Scrape
            Unparsed scrapes

        """

        unparsed_scrapes = self._search_table(status='X', parsed='F')

        return unparsed_scrapes

    @property
    def unparsed_summary(self):
        """
        Formatted summary of all unparsed scrapes

        Returns
        -------
        _summary : str
            Summary of all unparsed scrapes

        """

        _summary = reports.unparsed_scrapes(scrapes=self)

        return _summary

    def add_entry(self, **kwargs):
        """
        Initializes an entry for a scrape

        Keyword Arguments
        -----------------
        label : str
            Label of the scrape job
        action_index : int
            Index of the URL in the input
        date : str
            Date and time of the scrape
        url : str
            Inputted URL
        journal : str
            Internal journal code of the URL
        strategy : str
            Internal code of the strategy used for scraping
        method : str
            Internal name of the method used for scraping

        """

        data = ({
            'label': kwargs['label'],
            'action_index': kwargs['action_index'],
            'date': kwargs['date'],
            'url': kwargs['url'],
            'journal': kwargs['journal'],
            'strategy': kwargs['strategy'],
            'method': kwargs['method'],
            'success': 'F',
            'status': 'W',
            'out_file': 'no_file',
            'parsed': 'F'
            })

        self._sanity_check()

        add_query = """
        INSERT INTO scrapes VALUES(:label, :action_index, :date, :url, :journal, :strategy, :method, :success, :status, :out_file, :parsed)
        """

        self._cur.execute(add_query, data)

        self._con.commit()

    def update_entry(self, **kwargs):
        """
        Given a ``label`` and ``action_index``, updates the corresponding 
            ``column_name`` value with ``new_value`` in the ``scrape`` table

        ``column_name`` must be in ['date', 'journal', 'strategy', 'status',
            'success', 'out_file', 'parsed']

        Keyword Arguments
        -----------------
        label : str
            Label of the job that the scrape belongs to
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

            case 'strategy':

                # TODO: strategies in the ScrapePlan will be revised
                # if False is left as a placeholder
                if False:
                    raise ValueError(f'Cannot update the scrape database. Invalid strategy={new_value} given; must be ...')

                else:

                    self._cur.execute("""
                    UPDATE scrapes SET strategy = ? WHERE label = ? AND action_index = ?
                    """, (new_value, label, action_index))

                    self._con.commit()

            case 'method':

                # TODO: strategies in the ScrapePlan will be revised
                # if False is left as a placeholder
                if False:
                    raise ValueError(f'Cannot update the scrape database. Invalid method={new_value} given; must be ...')

                else:

                    self._cur.execute("""
                    UPDATE scrapes SET method = ? WHERE label = ? AND action_index = ?
                    """, (new_value, label, action_index))

                    self._con.commit()

            case 'journal':

                # TODO: journals in the ScrapePlan will be revised
                # if False is left as a placeholder
                if False:
                    raise ValueError(f'Cannot update the scrape database. Invalid journal={new_value} given; must be ...')

                else:

                    self._cur.execute("""
                    UPDATE scrapes SET journal = ? WHERE label = ? AND action_index = ?
                    """, (new_value, label, action_index))

                    self._con.commit()

            case 'success':

                if not new_value in ('T', 'F'):
                    raise ValueError(f'Cannot update the scrape database. Invalid success={new_value} given; must be one of ("T", "F").')

                self._cur.execute("""
                UPDATE scrapes SET success = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'status':

                if not new_value in ('I', 'W', 'R', 'E', 'X'):
                    raise ValueError(f'Cannot update the scrape database. Invalid status={new_value} given; must be one of ("I", "W", "R", "E", "X").')

                self._cur.execute("""
                UPDATE scrapes SET status = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'out_file':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the scrape database. Invalid out_file={new_value} given; must be a string.')

                self._cur.execute("""
                UPDATE scrapes SET out_file = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case 'date':

                if not isinstance(new_value, str):
                    raise ValueError(f'Cannot update the scrape database. Invalid date={new_value} given; must be a string')

                self._cur.execute("""
                UPDATE scrapes SET date = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

            case 'parsed':

                if new_value not in ['T', 'F']:
                    raise ValueError(f'Cannot update the scrape database. Invalid parsed={new_value} given; must be "T" or "F".')

                self._cur.execute("""
                UPDATE scrapes SET parsed = ? WHERE label = ? AND action_index = ?
                """, (new_value, label, action_index))

                self._con.commit()

            case _:

                self._con.close()
                raise ValueError(f'Cannot update the scrape database. Invalid column name "{column_name}" given.')

        self._con.close()

    def delete_entry(self, **kwargs):
        """
        Deletes an entry from the ``scrapes`` table with a
        given label and action_index

        Keyword Arguments
        -----------------
        label : str
            Label of the scrape job that the scrape belongs to
        action_index : int
            Index of the scrape

        Returns
        -------
        success : bool
            True if entry was removed, False if it was not

        """

        label = kwargs['label']
        action_index = kwargs['action_index']

        click.echo(f'Removing scrape {action_index} from job {label} from the scrape database ...')

        exists = self.scrape_exists(label, action_index)

        if not exists:
            click.echo(f'The entry for scrape {action_index} for job {label} does not exist.')
            success = False

        else:

            self._cur.execute("""
            DELETE FROM scrapes WHERE (label = ?) AND (action_index = ?)
            """, (label, action_index))

            self._con.commit()

            if not self.scrape_exists(label, action_index):

                click.echo(f'Entry {action_index} removed from job {label}')
                success = True

            else:

                click.echo(f'Could not delete entry {label}')
                success = False

        return success

    def scrape_exists(self, label, action_index):
        """
        Checks whether the scrape with ``label`` and ``action_index``
        exists in the table

        Parameters
        ----------
        label : str
            Label of the scrape job that scrape belongs to
        action_index : int
            Index of the scrape

        Returns
        -------
        exists : bool
            True if scrape exists, False if it does not

        """

        scrape = self.get_scrape(label=label, action_index=action_index)

        exists = bool(scrape)

        return exists

    def get_scrape(self, label, action_index):
        """
        Returns an instance of the ``self._Scrape`` named tuple
        for a scrape job with the given ``label`` and ``action_index``
        
        Parameters
        ----------
        label : str
            Label of the scrape job that scrape belongs to
        action_index : int
            Index of the scrape

        Returns
        -------
        scrape : appeer.db.tables.scrapes._Scrape
            The sought scrape

        """

        scrape = self._search_table(label=label, action_index=action_index)

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
