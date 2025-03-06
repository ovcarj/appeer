"""Defines the ``appeer pjob`` CLI"""

import sys

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

            appeer pjob new -E -j "my_label" -s "My description"

        """)
@click.option('-A', '--automatic', 'mode', flag_value='A', default=True,
        help="Parse mode A [default]")
@click.option('-E', '--everything', 'mode', flag_value='E',
        help='Parse mode E')
@click.option('-S', '--scrape_jobs', 'mode', flag_value='S',
        help='Parse mode S')
@click.option('-F', '--file_list', 'mode', flag_value='F',
        help='Parse mode F')
@click.option('-j', '--job_label', help='Parse job label')
@click.option('-s', '--description', 'description',
        help="Optional description of the parse job")
@click.option('-l', '--log_directory',
        help="Directory in which to store the log")
@click.option('-t', '--tmp_directory',
        type=click.Path(file_okay=False, writable=True),
        help="Directory into which to unpack files")
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

                appeer pjob add -j parse_job_label file_1.html file_2.xml

                appeer pjob add -j parse_job_label *
        
        """)
@click.option('-j', '--job_label', help='Parse job label', required=True)
@click.argument('inputs', nargs=-1)
def add(job_label, inputs):
    """
    Add publications to a preexisting parse job

    """

    data_source = list(inputs) or None

    pj = ParseJob(label=job_label)

    if not pj._job_exists: #pylint:disable=protected-access
        click.echo(f'Error: Cannot add publications for parsing; parse job "{job_label}" does not exist.')
        sys.exit()

    parse_mode = pj.mode

    if (data_source and parse_mode in ('A', 'E')):
        click.echo('Error: No inputs should be provided for parse modes "A" and "E".')
        sys.exit()

    if (not data_source and parse_mode in ('S', 'F')):
        click.echo('Error: Inputs must be provided for parse modes "S" and "F".')
        sys.exit()

    parse_scripts.append_publications(label=job_label,
            data_source=data_source)
