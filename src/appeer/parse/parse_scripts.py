"""Basic parse job scripts"""

from appeer.parse.parse_job import ParseJob

def create_new_job(**kwargs):
    """
    Create a new parse job

    Keyword Arguments
    -----------------
    label : str
        Unique job label
    description : str
        Optional job description
    log_directory : str
        Directory into which to store the log file
    parse_directory : str
        Directory into which to (temporarily) create files for parsing
    mode : str
        Parsing mode; one of ('E', 'A', 'S', F); 'A' is default
 
    Returns
    -------
    pj : appeer.scrape.scrape_job.ParseJob
        Parse job instance with the label set to the new job label

    """

    pj = ParseJob(job_mode='write')
    pj.new_job(**kwargs)

    return pj

def append_publications(label, data_source):
    """
    Add ``publications`` to a preexisting job

    Parameters
    ----------
    label : str
        Unique job label
    data_source : None | list of str
        None for modes ('A', 'E'), list of scrape job labels ('S'),
            list of file paths ('F')

    Returns
    -------
    pj : appeer.scrape.scrape_job.ParseJob
        Parse job instance with added publications

    """

    pj = ParseJob(label=label, job_mode='write')
    pj.add_publications(data_source=data_source)

    return pj
