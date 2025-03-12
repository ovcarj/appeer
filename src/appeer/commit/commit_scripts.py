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
