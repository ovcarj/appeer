import appeer.log
import click

from appeer.datadir import Datadir

def create_directories():
    """
    Create the ``appeer`` data directories.
    """

    ad = Datadir()
    ad.create_directories()

def initialize_appeer():
    """
    Create ``appeer`` data directories and #TODO databases.
    """

    start_datetime = appeer.utils.get_current_datetime()

    start_report = appeer.log.appeer_start(start_datetime)

    dashes = appeer.log.get_log_dashes()

    click.echo(start_report)

    create_directories()
    
    click.echo('appeer initialization complete!')

    end_report = appeer.log.appeer_end(start_datetime)
    click.echo(end_report)

if __name__ == '__main__':

    initialize_appeer()
