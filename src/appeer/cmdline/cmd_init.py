import click

from appeer.initialize import initialize_appeer

@click.command('init', help='Create appeer data directories and databases')
def init_cli():
    initialize_appeer()
