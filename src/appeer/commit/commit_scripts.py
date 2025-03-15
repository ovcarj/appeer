"""Basic commit job scripts"""

from appeer.commit.commit_job import CommitJob

def create_new_job(**kwargs):
    """
    Create a new commit job

    Keyword Arguments
    -----------------
    label : str
        Unique job label
    description : str
        Optional job description
    log_directory : str
        Directory into which to store the log file
    mode : str
        Parsing mode; one of ('A', 'E', 'P'); 'A' is default

    Returns
    -------
    cj : appeer.commit.commit_job.CommitJob
        Commit job instance with the label set to the new job label

    """

    cj = CommitJob(job_mode='write')
    cj.new_job(**kwargs)

    return cj

def append_publications(label, data_source):
    """
    Add ``publications`` to a preexisting job

    Parameters
    ----------
    label : str
        Unique job label
    data_source : None | list of str
        None for modes ('A', 'E'), list of parse job labels ('P'),

    Returns
    -------
    cj : appeer.parse.parse_job.CommitJob
        Commit job instance with added publications

    """

    cj = CommitJob(label=label, job_mode='write')
    cj.add_publications(data_source=data_source)

    return cj

def run_job(label,
            restart_mode='from_scratch',
            **kwargs):
    """
    Run the commit job with the given ``label``

    Parameters
    ----------
    label : str
        Unique job label
    restart_mode : str
        Must be in ('from_scratch', 'resume')

    Keyword Arguments
    -----------------
    no_parse_mark : bool
        If True, parse jobs will not be labeled as committed
            even if they are committed successfully
    overwrite : bool
        If False, ignore a duplicate DOI entry (default);
            if True, overwrite a duplicate DOI entry;
            if the given DOI is unique, this parameter has no impact

    """

    cj = CommitJob(label=label, job_mode='write')
    cj.run_job(restart_mode=restart_mode,
               **kwargs)

def create_and_run(data_source,
        label=None,
        **kwargs):
    """
    Create and run a commit job

    For more details on the parameters, see documentation of ParseJob

    Parameters
    ----------
    data_source : None | list of str
        None for modes ('A', 'E'),
            list of parse job labels ('P'),
    label : str
        Unique job label

    Keyword Arguments
    -----------------
    description : str
        Optional job description
    log_directory : str
        Directory into which to store the log file
    mode : str
        Parsing mode; one of ('A', 'E', 'P'); 'A' is default
    no_parse_mark : bool
        If True, parse jobs will not be labeled as committed
            even if they are committed successfully

    """

    cj = CommitJob(job_mode='write')

    cj.new_job(label=label, **kwargs)

    cj.add_publications(data_source=data_source)

    cj.run_job(restart_mode='from_scratch', **kwargs)
