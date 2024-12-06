"""
Initializes the ``appeer`` configuration file, databases 
and creates all the default directories
"""

import click

from appeer.general import log
from appeer.general import utils

from appeer.general.datadir import Datadir
from appeer.general.config import Config
from appeer.db.jobs_db import JobsDB
from appeer.db.pubs_db import PubsDB

def create_config():
    """
    Create the ``appeer`` config file.

    """

    cfg = Config()
    cfg.create_config_file()

def create_directories():
    """
    Create the ``appeer`` data directories.

    """

    ad = Datadir()
    ad.create_directories()

def create_databases():
    """
    Create the ``appeer`` databases.

    """

    jobs_db = JobsDB()
    jobs_db.create_database()

    pubs_db = PubsDB()
    pubs_db.create_database()

def initialize_appeer():
    """
    Create ``appeer`` data directories and databases.
    
    """

    dashes = log.get_log_dashes()

    start_datetime = utils.get_current_datetime()

    start_report = log.appeer_start(start_datetime)
    click.echo(start_report)

    create_config()
    create_directories()

    click.echo(dashes)

    create_databases()

    click.echo(dashes)

    click.echo('appeer initialization complete!')

    end_report = log.appeer_end(start_datetime)
    click.echo(end_report)

if __name__ == '__main__':

    initialize_appeer()
