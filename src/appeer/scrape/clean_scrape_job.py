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

        click.echo(dashes)

        if data_deleted:

            entry_deleted = sdb.delete_job_entry(scrape_label)

            if entry_deleted:
                click.echo(f'Scrape job {scrape_label} removed!')
