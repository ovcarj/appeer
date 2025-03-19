"""Defines the ``appeer cjob`` CLI"""

import sys

import click

from appeer.db.jobs_db import JobsDB
from appeer.commit.commit_job import CommitJob
from appeer.commit import commit_scripts


@click.group('cjob', invoke_without_command=True,
        help="""Print summary and manipulate commit jobs

        Summary of all commit jobs:

            appeer cjob

        Details of a job with a particular label (TODO):

            appeer cjob -j <label>

        Initialize a new commit job:

            appeer cjob new --help

        Add publications to a preexisting job:

            appeer cjob add --help

        Run a commit job

            appeer cjob run --help

        Instructions for cleaning the commit database:

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

@cjob_cli.command('add',
        help="""Add publications to a preexisting commit job

        The [INPUTS] argument must be provided for commit job in mode 'P'.

        (1) Add publications automatically (modes 'A' and 'E'):

                appeer cjob add -j commit_job_label

        (2) Add publications from a list of parse jobs (mode 'P')

                appeer cjob add -j commit_job_label parse_2025_1 parse_2025_2

        """)
@click.option('-j', '--job_label', help='Commit job label', required=True)
@click.argument('inputs', nargs=-1)
def add(job_label, inputs):
    """
    Add publications to a preexisting commit job

    """

    data_source = list(inputs) or None

    cj = CommitJob(label=job_label)

    if not cj._job_exists: #pylint:disable=protected-access
        click.echo(f'Error: Cannot add publications for committing; commit job "{job_label}" does not exist.')
        sys.exit()

    commit_mode = cj.mode

    if (data_source and commit_mode in ('A', 'E')):
        click.echo('Error: No inputs should be provided for commit modes "A" and "E".')
        sys.exit()

    if (not data_source and commit_mode == 'P'):
        click.echo('Error: Inputs must be provided for commit mode "P".')
        sys.exit()

    commit_scripts.append_publications(label=job_label,
            data_source=data_source)

@cjob_cli.command('run',
        help="""Run a preexisting commit job

        Example usage:

            appeer cjob run -j "my_label"

            appeer cjob run -r "resume" -c -j "my_label"

        Available run modes ("-r" flag): from_scratch/resume

            from_scratch: (Re)start committing publications from index=0

            resume: Resume a previously interrupted job

        """)
@click.option('-j', '--job_label', help='Parse job label', required=True)
@click.option('-r', '--restart_mode', help='Restart mode',
        default='from_scratch',
        show_default=True)
@click.option('-x', '--no_parse_mark',
        is_flag=True, default=False,
        help="Do not mark parse jobs as committed")
@click.option('-o', '--overwrite',
        is_flag=True, default=False,
        help="Overwrite existing entries")
def run(**kwargs):
    """
    Run a preexisting commit job

    """

    commit_scripts.run_job(label=kwargs['job_label'],
            **kwargs)
