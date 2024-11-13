import click

from appeer.db.scrape_db import ScrapeDB

@click.command()
def sdb_cli(name='sdb', help='Print scrape jobs summary'):
    """
    Print scrape jobs summary

    """
    
    scrape_db = ScrapeDB()
    scrape_db.print_jobs()

def main():
    sdb_cli()

if __name__ == '__main__':
    main()
