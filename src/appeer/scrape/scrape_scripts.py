"""Basic scrape job scripts"""

from appeer.general import utils as _utils
from appeer.scrape.scrape_job import ScrapeJob

def create_new_job(**kwargs):
    """
    Create a new scrape job

    Keyword Arguments
    -----------------
    label : str
        Unique job label
    description : str
        Optional job description
    log_directory : str
        Directory into which to store the log file
    download_directory : str
        Directory into which to download the data
    zip_file : str
        Path to the output ZIP file

    Returns
    -------
    sj : appeer.scrape.scrape_job.ScrapeJob
        Scrape job instance with the label set to the new job label

    """

    sj = ScrapeJob(job_mode='write')
    sj.new_job(**kwargs)

    return sj

def append_publications(label, publications):
    """
    Add ``publications`` to a preexisting job

    Parameters
    ----------
    label : str
        Unique job label
    publications : list | str
        List of URLs or path to a JSON or plaintext file
            containing URLs

    Returns
    -------
    sj : appeer.scrape.scrape_job.ScrapeJob
        Scrape job instance with appeded publications

    """
    sj = ScrapeJob(label=label, job_mode='write')
    sj.add_publications(publications=publications)

    return sj

def run_job(label,
            scrape_mode='from_scratch',
            cleanup=False,
            **kwargs):
    """
    Run the job with the given ``label``

    Parameters
    ----------
    label : str
        Unique job label
    scrape_mode : str
        Must be in ('from_scratch', 'resume')
    cleanup : bool
        If True, the final ZIP archive will be kept, while the directory
            containing the downloaded data will be deleted

    Keyword Arguments
    -----------------
    sleep_time : float
        Time (in seconds) between sending requests
    max_tries : int
        Maximum number of tries to get a response from an URL before
            giving up
    retry_sleep_time : float
        Time (in seconds) to wait before trying a nonresponsive URL again
    _429_sleep_time : float
        Time (in minutes) to wait if received a 429 status code

    """

    sj = ScrapeJob(label=label, job_mode='write')
    sj.run_job(scrape_mode=scrape_mode,
               cleanup=cleanup,
               **kwargs)

def create_and_run(publications,
                   label=None,
                   cleanup=False,
                   **kwargs):
    """
    Create and run a scrape job

    Parameters
    ----------
    publications : list | str
        List of URLs or path to a JSON or plaintext file
            containing URLs
    label : str
        Unique job label
    cleanup : bool
        If True, the final ZIP archive will be kept, while the directory
            containing the downloaded data will be deleted

    Keyword Arguments
    -----------------
    description : str
        Optional job description
    log_directory : str
        Directory into which to store the log file
    download_directory : str
        Directory into which to download the data
    zip_file : str
        Path to the output ZIP file
    sleep_time : float
        Time (in seconds) between sending requests
    max_tries : int
        Maximum number of tries to get a response from an URL before
            giving up
    retry_sleep_time : float
        Time (in seconds) to wait before trying a nonresponsive URL again
    _429_sleep_time : float
        Time (in minutes) to wait if received a 429 status code

    """

    sj = ScrapeJob(job_mode='write')

    sj.new_job(label=label, **kwargs)

    sj.add_publications(publications)

    sj.run_job(cleanup=cleanup,
               scrape_mode='from_scratch',
               **kwargs)

def get_execution_dict(job_labels):
    """
    Get a dictionary describing the status of a list of scrape jobs

    Parameters
    ----------
    job_labels : list of str
        List of scrape job labels

    Returns
    -------
    scrape_jobs_executed_dict : dict
        Dictionary of form {label1: status1, label2: status2, ...},
            where the keys are scrape job labels and the values are
            the job statuses

    """


    if not _utils.is_list_of_str(job_labels):
        raise TypeError('Scrape job labels must be provided as a list of strings or a single string.')

    if isinstance(job_labels, str):
        job_labels = [job_labels]

    try:

        sjs = [ScrapeJob(label=label) for label in job_labels]
        statuses = [sj.job_status if sj.job_status else 'N/A' for sj in sjs]

    # This means that an invalid job label was passed, so iterate the labels
    # one-by-one to find the error. This edge case is written as a seperate
    # block because it's slower to iterate instead of using list comprehension.
    except ValueError:

        statuses = []

        for label in job_labels:

            try:

                sj = ScrapeJob(label=label)

                if sj.job_status:
                    statuses.append(sj.job_status)

                else:
                    statuses.append('N/A')

            except ValueError:
                print('HERE')
                statuses.append('N/A')

    scrape_jobs_executed_dict = dict(zip(job_labels, statuses))

    return scrape_jobs_executed_dict
