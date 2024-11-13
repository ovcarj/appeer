import click

from appeer.db.scrape_db import ScrapeDB

@click.command('sdb', help="""Print scrape jobs summary

        To view a summary of all scrape jobs, use:

        appeer sdb

        To see details of a job with a particular label:

        appeer sdb <label>

        To print all currently unparsed scrapes:

        appeer sdb -u

        Instructions for cleaning the scrape database:

        appeer clean sjob --help

        """)
@click.option('-u', '--unparsed', is_flag=True, default=False, help='Print all unparsed scrapes')
@click.argument('label', nargs=1, required=False)
def sdb_cli(label, unparsed):
    """
    Print scrape jobs summary

    """

    scrape_db = ScrapeDB()

    if unparsed:
        scrape_db.print_all_unparsed()

    elif label:
        scrape_db.print_job_details(label)

    else:
        scrape_db.print_jobs()

def main():
    sdb_cli()

if __name__ == '__main__':
    main()
