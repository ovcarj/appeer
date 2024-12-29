"""Defines the ``appeer pjob`` CLI"""

import click

from appeer.db.jobs_db import JobsDB

@click.command('pjob', help="""Print parse jobs summary

        View a summary of all parse jobs, use:

            appeer pjob

        See details of a job with a particular label:

            appeer pjob <label>

        Print all not yet committed parsed publications:

            appeer pjob -u

        Instructions for cleaning the parse database:

            appeer clean pjob --help

        """)
@click.option('-u', '--uncommitted', is_flag=True,
        default=False, help='Print not yet committed entries ')
@click.argument('label', nargs=1, required=False)
def pjob_cli(label, uncommitted):
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
