"""Defines the ``appeer init`` CLI"""

import click

from appeer.general.initialize import initialize_appeer

@click.command('init', help='Initialize appeer data directories and databases')
def init_cli():
    """
    Initialize data directories and databases
    
    """

    initialize_appeer()
