import click

import appeer.log
import appeer.utils

from appeer.datadir import Datadir
from appeer.db.jobs_db import JobsDB

from appeer.parse.parsing_flow import initialize_parse_job

def pack_unparsed():
    """
    Searches the scrape database for unparsed scrapes and returns the data
    in a convenient form for batch parsing (dictionary of scrape job labels, 
    ZIP file paths, and the corresponding scrapes).

    An unparsed scrape is defined by having the 'X' status and 'F' parse flag.

    Returns
    -------
    unparsed_pack : list
        List of dictionaries of scrape job labels, ZIP files and the 
        corresponding scrapes

    """

    unparsed_pack = []

    jobs_db = JobsDB()

    unparsed_scrapes = jobs_db._get_all_unparsed()

    unique_job_labels = list(set([scrape.label for scrape in unparsed_scrapes]))

    if unparsed_scrapes:

        unparsed_pack = [
                {
                f'{job_label}':
                    {
                    'zip_file': jobs_db._get_scrape_job(job_label).zip_file,
                    'scrapes': [scrape for scrape in unparsed_scrapes if scrape.label == job_label]
                    }
                }
                for job_label in unique_job_labels
                ]

    else:

        unparsed_pack = []

    return unparsed_pack

def parse_automatic(description=None,
        logdir=None, parse_directory=None, 
        commit=False, cleanup=True):
    """
    Searches the scrape database for unparsed files, performs the parsing,
    and updates the parse/scrape databases accordingly.

    Parameters
    ----------

    description : str
        Optional description of the parse job
    logdir : str
        Directory in which to store the log. If not given, the default appeer
        data directory is used
    parse_directory : str
        Path to the directory where (temporary) files for parsing are created
    commit : bool
        If True, the results of the parse job will be added to the pub.db database
    cleanup : bool
        If True, delete all temporary files after the parse job is done

    """
    
    parse_label, _logger, start_datetime = initialize_parse_job(
            mode='A',
            description=description,
            logdir=logdir, parse_directory=parse_directory
            )

    unparsed_pack = pack_unparsed()

    no_of_scrape_jobs = len(unparsed_pack)
