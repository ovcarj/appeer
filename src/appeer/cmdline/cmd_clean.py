import click

from appeer.datadir import Datadir
from appeer.config import Config

@click.command('all_data', help='Delete the appeer directory')
def clean_all_data():
    """
    Deletes all ``appeer`` data directories
    """

    ad = Datadir()
    ad.clean_all_directories()

@click.command('downloads', help='Delete contents of the appeer/downloads directory')
def clean_downloads():
    """
    Deletes the contents of the ``appeer/downloads`` directory
    """

    ad = Datadir()
    ad.clean_downloads()

@click.command('scrape_archives', help='Delete contents of the appeer/scrape_archives directory')
def clean_scrape_archives():
    """
    Deletes the contents of the ``appeer/scrape_archives`` directory
    """

    ad = Datadir()
    ad.clean_scrape_archives()

@click.command('scrape_logs', help='Delete contents of the appeer/scrape_logs directory')
def clean_scrape_logs():
    """
    Deletes the contents of the ``appeer/scrape_logs`` directory
    """

    ad = Datadir()
    ad.clean_scrape_logs()

@click.command('parse', help='Delete contents of the appeer/parse directory')
def clean_parse():
    """
    Deletes the contents of the ``appeer/parse`` directory
    """

    ad = Datadir()
    ad.clean_parse()

@click.command('parse_logs', help='Delete contents of the appeer/parse_logs directory')
def clean_parse_logs():
    """
    Deletes the contents of the ``appeer/parse_logs`` directory
    """

    ad = Datadir()
    ad.clean_parse_logs()

@click.command('db', help='Delete contents of the appeer/db directory')
def clean_db():
    """
    Deletes the contents of the ``appeer/db`` directory
    """

    ad = Datadir()
    ad.clean_db()

@click.command('config', help='Delete the appeer config file')
def clean_config():
    """
    Deletes the ``appeer`` config file.
    """

    cfg = Config()
    cfg.clean_config()

@click.group()
def clean_cli(name='clean', help='Delete contents of the appeer data directory'):
    """
    Delete contents of the appeer data directory

    Example usage:

    appeer clean downloads

    """
    pass

clean_cli.add_command(clean_all_data)
clean_cli.add_command(clean_downloads)
clean_cli.add_command(clean_scrape_archives)
clean_cli.add_command(clean_scrape_logs)
clean_cli.add_command(clean_parse)
clean_cli.add_command(clean_parse_logs)
clean_cli.add_command(clean_db)

clean_cli.add_command(clean_config)

def main():
    clean_cli()

if __name__ == '__main__':
    main()
