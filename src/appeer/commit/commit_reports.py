"""Generate appeer commit-related reports"""

import appeer.general.log as _log
import appeer.general.utils as _utils


def commit_general_report(job, add_status_info=False):
    """
    Return a formatted report containing general information of a commit job

    Parameters
    ----------
    job : appeer.commit.commit_job.CommitJob
        appeer commit job
    add_status_info : bool
        If True, job status and succ./tot. are added

    Returns
    -------
    report : str
        General information on a commit job

    """

    report = ""

    report += _log.boxed_message(f'COMMIT JOB: {job.label}', centered=True) + '\n'
    report += _log.center(f'Created on {_utils.human_datetime(job.date)}') + '\n'
    report += _log.center(job.description) + '\n\n'

    msg = ''

    msg += f'{"Log":12} {job.log}\n'
    msg += f'{"Mode":12} {job.mode}'

    if add_status_info:
        msg += '\n'
        msg += f'{"Job status":12} {job.job_status}\n'
        msg += f'{"Succ./Tot.":12} {job.job_successes}/{job.no_of_publications}'

    report += _log.boxed_message(msg) + '\n'

    return report
