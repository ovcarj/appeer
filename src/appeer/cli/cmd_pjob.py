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

        Example usage:

            ...
        
        """)
@click.argument('filename')
@click.option('-j', '--job_label', help='Parse job label', required=True)
def add(filename, job_label):
    """
    Add publications to a preexisting parse job

    """

#    parse_scripts.append_publications(label=job_label, publications=filename)

@pjob_cli.command('run',
        help="""Run a parse job

        Example usage:
        
            appeer pjob run -j "my_label"

            appeer pjob run -r "resume" -c -j "my_label"

        Available run modes ("-r" flag): from_scratch/resume
        
            from_scratch: (Re)start parsing publications from index=0

            resume: Resume a previously interrupted job


        """)
@click.option('-j', '--job_label', help='Parse job label', required=True)
@click.option('-r', '--run_mode', help='Run mode', default='from_scratch', show_default=True)
@click.option('-c', '--cleanup',
        is_flag=True, default=False,
        help="Delete the directory containing the downloaded data after job ends") #pylint:disable=line-too-long
def run(**kwargs):
    """
    Add publications to a preexisting parse job

    """

    label = kwargs['job_label']
    parse_mode = kwargs['run_mode']
    cleanup = kwargs['cleanup']

#    parse_scripts.run_job(label=label,
#            parse_mode=parse_mode,
#            cleanup=cleanup)
