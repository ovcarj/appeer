"""Defines the ``appeer cjob`` CLI"""

import sys

import click

from appeer.db.jobs_db import JobsDB
from appeer.commit.commit_job import CommitJob
from appeer.commit import commit_scripts


@click.group('cjob', invoke_without_command=True,
        help="""Print summary and manipulate commit jobs

        Summary of all commit jobs (TODO):

            appeer cjob

        Details of a job with a particular label (TODO):

            appeer cjob -j <label>

        Initialize a new commit job:

            appeer cjob new --help

        Add publications to a preexisting job (TODO):

            appeer cjob add --help

        Run a commit job (TODO)

            appeer cjob run --help

        Instructions for cleaning the commit database (TODO):

            appeer clean cjob --help

        """)
@click.option('-j', '--job_label',
        help='Commit job label')
@click.pass_context
def cjob_cli(ctx, job_label):
    """
    Print summary and manipulate commit jobs

    """

    if ctx.invoked_subcommand is None:

        jobs_db = JobsDB()

        if job_label:
            cj = CommitJob(job_label)
            click.echo(cj.summary)

        else:
            click.echo(jobs_db.commit_jobs.summary)

@cjob_cli.command('new',
        help="""Initialize an empty commit job

        Example usage:
        
            appeer cjob new

            appeer cjob new -E -j "my_label" -s "My description"

        """)
@click.option('-A', '--automatic', 'mode', flag_value='A', default=True,
        help="Commit mode A [default]")
@click.option('-E', '--everything', 'mode', flag_value='E',
        help='Commit mode E')
@click.option('-P', '--parse_jobs', 'mode', flag_value='P',
        help='Commit mode P')
@click.option('-j', '--job_label', help='Commit job label')
@click.option('-s', '--description', 'description',
        help="Optional description of the commit job")
@click.option('-l', '--log_directory',
        help="Directory in which to store the log")
def new(**kwargs):
    """
    Initialize an empty commit job

    """

    kwargs['label'] = kwargs['job_label']

    commit_scripts.create_new_job(**kwargs)
