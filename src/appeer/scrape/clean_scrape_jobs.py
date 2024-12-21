"""Deletes scrape jobs"""

import click

from appeer.general import log

from appeer.general.datadir import Datadir
from appeer.db.jobs_db import JobsDB

def clean_scrape_job(scrape_label):
    """
    Deletes all data associated with the scrape job with the given label

    Parameters
    ----------
    scrape_label : str
        Label of the scrape job whose data is being deleted

    """

    dashes = log.get_log_dashes()

    jdb = JobsDB()

    job_exists = jdb.scrape_jobs.job_exists(scrape_label)

    if not job_exists:
        click.echo(f'Scrape job {scrape_label} does not exist.')

    else:

        scrape_job = jdb.scrape_jobs.get_job(label=scrape_label)

        datadir = Datadir()

        data_deleted = datadir.clean_scrape_job_data(
                scrape_label=scrape_label,
                download_directory=scrape_job.download_directory,
                zip_file=scrape_job.zip_file,
                log=scrape_job.log
                )

        if data_deleted:

            entry_deleted = jdb.scrape_jobs.delete_entry(label=scrape_label)

            if entry_deleted:
                click.echo(dashes)
                click.echo(f'Job {scrape_label} removed!')

        click.echo(dashes + '\n')

def clean_scrape_jobs(scrape_labels):
    """
    Deletes all data associated with a list of scrape jobs.

    Parameters
    ----------
    scrape_labels : list
        List of scrape labels whose data is being deleted

    """

    for scrape_label in scrape_labels:
        clean_scrape_job(scrape_label)

def clean_bad_jobs():
    """
    Deletes all data associated with jobs for which the job status is not 'X'.

    """

    jdb = JobsDB()

    bad_jobs = jdb.scrape_jobs.bad_jobs

    bad_labels = [bad_job.label for bad_job in bad_jobs]

    clean_scrape_jobs(bad_labels)

def clean_all_jobs():
    """
    Deletes all data for all jobs in the scrape database.

    """

    jdb = JobsDB()

    labels = [job.label for job in jdb.scrape_jobs.entries]

    clean_scrape_jobs(labels)
