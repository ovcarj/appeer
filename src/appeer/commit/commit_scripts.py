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
