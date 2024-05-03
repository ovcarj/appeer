import click

from appeer.datadir import Datadir

@click.command('all', help='Delete the appeer data directory.')
def clean_all():
    """
    Deletes all ``appeer`` data directories
    """

    ad = Datadir()
    ad.clean_all_directories()

@click.command('downloads', help='Delete contents of the appeer/downloads data directory.')
def clean_downloads():
    """
    Deletes the contents of the ``appeer/downloads`` data directory
    """

    ad = Datadir()
    ad.clean_downloads()

@click.command('scrape_archives', help='Delete contents of the appeer/scrape_archives data directory.')
def clean_scrape_archives():
    """
    Deletes the contents of the ``appeer/scrape_archives`` data directory
    """

    ad = Datadir()
    ad.clean_scrape_archives()

@click.command('scrape_logs', help='Delete contents of the appeer/scrape_logs data directory.')
def clean_scrape_logs():
    """
    Deletes the contents of the ``appeer/scrape_logs`` data directory
    """

    ad = Datadir()
    ad.clean_scrape_logs()

@click.command('parse', help='Delete contents of the appeer/parse data directory.')
def clean_parse():
    """
    Deletes the contents of the ``appeer/parse`` data directory
    """

    ad = Datadir()
    ad.clean_parse()

@click.command('parse_logs', help='Delete contents of the appeer/parse_logs data directory.')
def clean_parse_logs():
    """
    Deletes the contents of the ``appeer/parse_logs`` data directory
    """

    ad = Datadir()
    ad.clean_parse_logs()

@click.command('db', help='Delete contents of the appeer/db data directory.')
def clean_db():
    """
    Deletes the contents of the ``appeer/db`` data directory
    """

    ad = Datadir()
    ad.clean_db()

@click.group()
def clean_cli(name='clean', help='Tools for cleaning the appeer data directory'):
    """
    Tools for cleaning the appeer data directory
    """
    pass

clean_cli.add_command(clean_all)
clean_cli.add_command(clean_downloads)
clean_cli.add_command(clean_scrape_archives)
clean_cli.add_command(clean_scrape_logs)
clean_cli.add_command(clean_parse)
clean_cli.add_command(clean_parse_logs)
clean_cli.add_command(clean_db)

def main():
    clean_cli()

if __name__ == '__main__':
    main()
