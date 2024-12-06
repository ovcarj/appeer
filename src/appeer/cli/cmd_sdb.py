"""
Defines the ``appeer sdb`` CLI
"""

import click

from appeer.db.jobs_db import JobsDB

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

    jobs_db = JobsDB()

    if unparsed:
        jobs_db.scrapes.print_unparsed()

    elif label:
        jobs_db.scrape_jobs.print_job_details(label=label)

    else:
        jobs_db.scrape_jobs.print_summary()
