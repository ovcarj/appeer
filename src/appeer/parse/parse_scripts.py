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
