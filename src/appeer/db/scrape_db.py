import click

from appeer.db.db import DB

import appeer.utils as appeer_log

class ScrapeDB(DB):
    """
    Handles the ``scrape.db`` database.

    """

    def __init__(self):
        """
        If the scrape database exists, establishes a connection and a cursor.

        """

        super().__init__(db_type='scrape')

    def _initialize_database(self):
        """
        Initializes the SQL tables when the ``scrape`` database is created.

        """

        self._cur.execute('CREATE TABLE scrape_runs(scrape_label, scrape_description, scrape_log, scrape_zip, scrape_date, scrape_clean_exit, scrape_run_successes, scrape_run_fails, scrape_run_parsed)')
        self._cur.execute('CREATE TABLE scrape(scrape_label, scrape_success_status, scrape_file, scrape_parsed)')
        self._con.commit()
