"""Handles the jobs database, which includes scrape, parse and commit jobs"""

from appeer.db.db import DB

class JobsDB(DB, tables=[
        'scrape_jobs',
        'scrapes',
        'parse_jobs',
        'parses',
        'commit_jobs',
        'commits'
    ]):
    """
    Handles the ``jobs.db`` database

    """

    def __init__(self):
        """
        If the jobs database exists, establishes a connection and a cursor

        """

        super().__init__(db_type='jobs')
