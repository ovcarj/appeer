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
        Parsing mode; one of ('A', 'E', 'S', F); 'A' is default
 
    Returns
    -------
    pj : appeer.scrape.scrape_job.ParseJob
        Parse job instance with the label set to the new job label

    """

    pj = ParseJob(job_mode='write')
    pj.new_job(**kwargs)

    return pj

def run_job(label,
            restart_mode='from_scratch',
            cleanup=False,
            **kwargs):
    """
    Run the parse job with the given ``label``

    Parameters
    ----------
    label : str
        Unique job label
    restart_mode : str
        Must be in ('from_scratch', 'resume')
    cleanup : bool
        If True, delete the temporary parsing directory
            upon completion of the parse job

    Keyword Arguments
    -----------------
    no_scrape_mark : bool
        If True, scrape jobs will not be labeled as parsed
            even if they are parsed successfully
    publishers : str | list of str | None
        List of candidate parser publisher codes
    journals : str | list of str | None
        List of candidate parser journal codes
    data_types : str | list of str | None
        List of candidate parser data types;
            currently, only 'txt' is supported

    """

    pj = ParseJob(label=label, job_mode='write')
    pj.run_job(restart_mode=restart_mode,
               cleanup=cleanup,
               **kwargs)

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

def create_and_run(data_source,
        label=None, cleanup=False,
        **kwargs):
    """
    Create and run a parse job

    For more details on the parameters, see documentation of ParseJob

    Parameters
    ----------
    data_source : None | list of str
        None for modes ('A', 'E'),
            list of scrape job labels ('S'),
            list of file paths ('F')
    label : str
        Unique job label
    cleanup : bool
        If True, the temporary parse directory will be deleted at the
            end of the parse job

    Keyword Arguments
    -----------------
    description : str
        Optional job description
    log_directory : str
        Directory into which to store the log file
    parse_directory : str
        Directory into which to (temporarily) create files for parsing
    mode : str
        Parsing mode; one of ('A', 'E', 'S', F); 'A' is default
    publishers : str | list of str | None
        List of candidate parser publisher codes
    journals : str | list of str | None
        List of candidate parser journal codes
    data_types : str | list of str | None
        List of candidate parser data types;
            currently, only 'txt' is supported
    no_scrape_mark : bool
        If True, scrape jobs will not be labeled as parsed
            even if they are parsed successfully

    """

    pj = ParseJob(job_mode='write')

    pj.new_job(label=label, **kwargs)

    pj.add_publications(data_source=data_source)

    pj.run_job(restart_mode='from_scratch',
            cleanup=cleanup,
            **kwargs)
