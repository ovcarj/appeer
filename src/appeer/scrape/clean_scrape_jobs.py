import click

import appeer.log

from appeer.datadir import Datadir
from appeer.db.scrape_db import ScrapeDB

def clean_scrape_job(scrape_label):
    """
    Deletes all data associated with the scrape job with the label ``scrape_label``.

    Parameters
    ----------
    scrape_label : str
        Label of the scrape job whose data is being deleted

    """

    dashes = appeer.log.get_log_dashes()
    short_dashes = appeer.log.get_short_log_dashes()

    sdb = ScrapeDB()

    job_exists = sdb._scrape_job_exists(scrape_label)

    if not job_exists:
        click.echo(f'Scrape job {scrape_label} does not exist.')

    else:

        scrape_job = sdb._get_job(scrape_label)
    
        datadir = Datadir()
        data_deleted = datadir.clean_scrape_job_data(
                scrape_label=scrape_label,
                download_directory=scrape_job.download_directory,
                zip_file=scrape_job.zip_file,
                log=scrape_job.log
                )

        if data_deleted:

            entry_deleted = sdb.delete_job_entry(scrape_label)

            if entry_deleted:
                click.echo(f'Job {scrape_label} removed!')

        click.echo(dashes)

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

    sdb = ScrapeDB()

    bad_jobs = sdb._get_bad_jobs()

    bad_labels = [bad_job.label for bad_job in bad_jobs]

    clean_scrape_jobs(bad_labels)

def clean_all_jobs():
    """
    Deletes all data for all jobs in the scrape database.

    """

    sdb = ScrapeDB()
    sdb._get_jobs()

    labels = [job.label for job in sdb.scrape_jobs]

    clean_scrape_jobs(labels)
