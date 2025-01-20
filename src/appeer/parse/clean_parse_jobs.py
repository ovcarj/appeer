"""Deletes parse jobs"""

import click

from appeer.general import log

from appeer.general.datadir import Datadir
from appeer.db.jobs_db import JobsDB

def clean_parse_job(parse_label):
    """
    Deletes all data associated with the parse job with the given label

    Parameters
    ----------
    parse_label : str
        Label of the parse job whose data is being deleted

    """

    dashes = log.get_log_dashes()

    jdb = JobsDB()

    job_exists = jdb.parse_jobs.job_exists(label=parse_label)

    if not job_exists:
        click.echo(f'Parse job {parse_label} does not exist.')

    else:

        parse_job = jdb.parse_jobs.get_job(label=parse_label)

        datadir = Datadir()

        data_deleted = datadir.clean_parse_job_data(
                parse_label=parse_label,
                parse_directory=parse_job.parse_directory,
                log=parse_job.log
                )

        if data_deleted:

            entry_deleted = jdb.parse_jobs.delete_entry(label=parse_label)

            if entry_deleted:
                click.echo(dashes)
                click.echo(f'Job {parse_label} removed!')

        click.echo(dashes + '\n')

def clean_parse_jobs(parse_labels):
    """
    Deletes all data associated with a list of parse jobs.

    Parameters
    ----------
    parse_labels : list
        List of parse labels whose data is being deleted

    """

    for parse_label in parse_labels:
        clean_parse_job(parse_label)

def clean_bad_jobs():
    """
    Deletes all data associated with jobs for which the job status is not 'X'.

    """

    jdb = JobsDB()

    bad_jobs = jdb.parse_jobs.bad_jobs

    bad_labels = [bad_job.label for bad_job in bad_jobs]

    clean_parse_jobs(bad_labels)

def clean_all_jobs():
    """
    Deletes all data for all jobs in the parse database.

    """

    jdb = JobsDB()

    labels = [job.label for job in jdb.parse_jobs.entries]

    clean_parse_jobs(labels)
