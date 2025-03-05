"""Defines the ``appeer pjob`` CLI"""

import click

from appeer.db.jobs_db import JobsDB
from appeer.parse.parse_job import ParseJob
from appeer.parse import parse_scripts

@click.group('pjob', invoke_without_command=True,
        help="""Print summary and manipulate parse jobs

        Summary of all parse jobs:

            appeer pjob

        Details of a job with a particular label:

            appeer pjob -j <label>

        List all not yet committed parses:

            appeer pjob -u

        Initialize a new parse job:

            appeer pjob new --help

        Add publications to a preexisting job:

            appeer pjob add --help

        Run a parse job

            appeer pjob run --help

        Instructions for cleaning the parse database:

            appeer clean pjob --help

        """)
@click.option('-u', '--uncommitted', is_flag=True, default=False,
        help='List all not yet committed parses')
@click.option('-j', '--job_label',
        help='Parse job label')
@click.pass_context
def pjob_cli(ctx, job_label, uncommitted):
    """
    Print summary and manipulate parse jobs

    """

    if ctx.invoked_subcommand is None:

        jobs_db = JobsDB()

        if uncommitted:
            click.echo(jobs_db.parses.uncommitted_summary)

        elif job_label:
            pj = ParseJob(job_label)
#            click.echo(pj.summary)

        else:
            click.echo(jobs_db.parse_jobs.summary)

@pjob_cli.command('new',
        help="""Initialize an empty parse job

        Example usage:
        
            appeer pjob new

            appeer pjob new -j "my_label" -s "My description" -m "A"

        """)
@click.option('-j', '--job_label', help='Parse job label')
@click.option('-s', '--description', 'description',
        help="Optional description of the parse job")
@click.option('-m', '--mode', default='A', show_default=True,
        help="Parsing mode")
@click.option('-l', '--log_directory',
        help="Directory in which to store the log")
@click.option('-d', '--parse_directory',
        help="Directory in which to create files for parsing")
def new(**kwargs):
    """
    Initialize an empty parse job

    """

    kwargs['label'] = kwargs['job_label']

    parse_scripts.create_new_job(**kwargs)

@pjob_cli.command('add',
        help="""Add publications to a preexisting parse job

        The [INPUTS] argument must be provided for parse job in modes 'S' and 'F'.

        (1) Add publications automatically (modes 'A' and 'E'):

            appeer pjob add -j parse_job_label

        (2) Add publications from a list of scrape jobs (mode 'S')

            appeer pjob add -j parse_job_label scrape_2025_1 scrape_2025_2

        (3) Add publications from a list of file paths (mode 'F')

            appeer pjob add -j parse_job_label file_1.html -f file_2.xml

            appeer pjob add -j parse_job_label *
        
        """)
@click.option('-j', '--job_label', help='Parse job label', required=True)
@click.argument('inputs', nargs=-1)
def add(job_label, inputs):
    """
    Add publications to a preexisting parse job

    """

    data_source = list(inputs) or None

    parse_scripts.append_publications(label=job_label,
            data_source=data_source)
