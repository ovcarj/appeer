"""Deletes commit jobs"""

import click

from appeer.general import log

from appeer.general.datadir import Datadir
from appeer.db.jobs_db import JobsDB

def clean_commit_job(commit_label):
    """
    Deletes all data associated with the commit job with the given label

    Parameters
    ----------
    commit_label : str
        Label of the commit job whose data is being deleted

    """

    dashes = log.get_log_dashes()

    jdb = JobsDB()

    job_exists = jdb.commit_jobs.job_exists(label=commit_label)

    if not job_exists:
        click.echo(f'Commit job {commit_label} does not exist.')

    else:

        commit_job = jdb.commit_jobs.get_job(label=commit_label)

        datadir = Datadir()

        data_deleted = datadir.clean_commit_job_data(
                commit_label=commit_label,
                log=commit_job.log
                )

        if data_deleted:

            entry_deleted = jdb.commit_jobs.delete_entry(label=commit_label)

            if entry_deleted:
                click.echo(dashes)
                click.echo(f'Job {commit_label} removed!')

        click.echo(dashes + '\n')

def clean_commit_jobs(commit_labels):
    """
    Deletes all data associated with a list of commit jobs.

    Parameters
    ----------
    commit_labels : list
        List of commit labels whose data is being deleted

    """

    for commit_label in commit_labels:
        clean_commit_job(commit_label)

def clean_bad_jobs():
    """
    Deletes all data associated with jobs for which the job status is not 'X'.

    """

    jdb = JobsDB()

    bad_jobs = jdb.commit_jobs.bad_jobs

    bad_labels = [bad_job.label for bad_job in bad_jobs]

    clean_commit_jobs(bad_labels)

def clean_all_jobs():
    """
    Deletes all data for all jobs in the commit database.

    """

    jdb = JobsDB()

    labels = [job.label for job in jdb.commit_jobs.entries]

    clean_commit_jobs(labels)
