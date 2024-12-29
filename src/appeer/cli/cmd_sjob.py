"""Defines the ``appeer sjob`` CLI"""

import click

from appeer.db.jobs_db import JobsDB
from appeer.general.config import Config
from appeer.scrape import scrape_scripts
from appeer.scrape.scrape_job import ScrapeJob

settings = Config().settings

if settings:

    default_sleep_time = float(
            settings['ScrapeDefaults']['sleep_time'])
    default_max_tries = int(
            settings['ScrapeDefaults']['max_tries'])
    default_retry_sleep_time = float(
            settings['ScrapeDefaults']['retry_sleep_time'])

else:
    # most probably "appeer init" was not yet run
    default_sleep_time = 1.0
    default_max_tries = 3
    default_retry_sleep_time = 10.0

@click.group('sjob', invoke_without_command=True,
        help="""Print summary and manipulate scrape jobs

        Summary of all scrape jobs:

            appeer sjob

        Details of a job with a particular label:

            appeer sjob -j <label>

        List all currently unparsed scrapes:

            appeer sjob -u

        Initialize a new scrape job:

            appeer sjob new --help

        Add publications to a preexisting job:

            appeer sjob add --help

        Run a scrape job

            appeer sjob run --help

        Instructions for cleaning the scrape database:

            appeer clean sjob --help

        """)
@click.option('-u', '--unparsed', is_flag=True, default=False,
        help='Print all unparsed scrapes')
@click.option('-j', '--job_label',
        help='Scrape job label')
@click.pass_context
def sjob_cli(ctx, job_label, unparsed):
    """
    Print summary and manipulate scrape jobs

    """

    if ctx.invoked_subcommand is None:

        jobs_db = JobsDB()

        if unparsed:
            click.echo(jobs_db.scrapes.unparsed_summary)

        elif job_label:
            sj = ScrapeJob(job_label)
            click.echo(sj.summary)

        else:
            click.echo(jobs_db.scrape_jobs.summary)

@sjob_cli.command('new',
        help="""Initialize an empty scrape job

        Example usage:
        
            appeer sjob new

            appeer sjob new -j "my_label" -s "My description"

        """)
@click.option('-j', '--job_label', help='Scrape job label')
@click.option('-o', '--output', 'zip_file',
        help="Name of the ZIP archive containing the downloaded data")
@click.option('-s', '--description', 'description',
        help="Optional description of the scrape job")
@click.option('-l', '--log_directory',
        help="Directory in which to store the log")
@click.option('-d', '--download_directory',
        help="Directory into which to download the files")
def new(**kwargs):
    """
    Initialize an empty scrape job

    """

    kwargs['label'] = kwargs['job_label']

    scrape_scripts.create_new_job(**kwargs)

@sjob_cli.command('add',
        help="""Add publications to a preexisting scrape job

        Example usage:
        
            appeer sjob add -j "my_label" PoP.json

        The input file can be either a JSON file (e.g. a PoP.json file containing 'article_url' keys) or a plaintext file with each URL written in a new line.

        The URLs can be written either as a full URL or a DOI.
        E.g., the following entries are equally valid:

            https://pubs.rsc.org/en/content/articlelanding/2023/ob/d3ob00424d
            10.1039/D3OB00424D

        Valid entries start with 'https://' or need to be in DOI format (10.prefix/suffix).


        """)
@click.argument('filename')
@click.option('-j', '--job_label', help='Scrape job label', required=True)
def add(filename, job_label):
    """
    Add publications to a preexisting scrape job

    """

    scrape_scripts.append_publications(label=job_label, publications=filename)

@sjob_cli.command('run',
        help="""Run a scrape job

        Example usage:
        
            appeer sjob run -j "my_label"

            appeer sjob run -r "resume" -c -j "my_label"

        Available run modes ("-r" flag): from_scratch/resume
        
            from_scratch: (Re)start downloading publications from index=0

            resume: Resume a previously interrupted job


        """)
@click.option('-j', '--job_label', help='Scrape job label', required=True)
@click.option('-r', '--run_mode', help='Run mode', default='from_scratch', show_default=True)
@click.option('-c', '--cleanup',
        is_flag=True, default=False,
        help="Delete the directory containing the downloaded data after job ends") #pylint:disable=line-too-long
@click.option('-t', '--sleep_time',
        default=default_sleep_time, show_default=True,
        help="Time (in seconds) between sending requests")
@click.option('-m', '--max_tries',
        default=default_max_tries, show_default=True,
        help="Maximum number of tries to get a response from an URL before giving up") #pylint:disable=line-too-long
@click.option('-rt', '--retry_sleep_time',
        default=default_retry_sleep_time, show_default=True,
        help="Time (in seconds) between retrying a URL")
def run(**kwargs):
    """
    Add publications to a preexisting scrape job

    """

    label = kwargs['job_label']
    scrape_mode = kwargs['run_mode']
    cleanup = kwargs['cleanup']

    scrape_scripts.run_job(label=label,
            scrape_mode=scrape_mode,
            cleanup=cleanup)
