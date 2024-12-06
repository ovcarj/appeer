"""
Defines the ``appeer pdb`` CLI
"""

import click

from appeer.db.jobs_db import JobsDB

@click.command('pdb', help="""Print parse jobs summary

        To view a summary of all parse jobs, use:

        appeer pdb

        To see details of a job with a particular label:

        appeer pdb <label>

        To print all not yet committed parsed publications:

        appeer pdb -u

        Instructions for cleaning the parse database:

        appeer clean pjob --help

        """)
@click.option('-u', '--uncommitted', is_flag=True,
        default=False, help='Print not yet committed entries ')
@click.argument('label', nargs=1, required=False)
def pdb_cli(label, uncommitted):
    """
    Print parse jobs summary

    """

    jobs_db = JobsDB()

    if uncommitted:
        pass

    elif label:
        pass

    else:
        jobs_db.parse_jobs.print_summary()
