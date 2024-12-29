"""Generate appeer parse-related reports"""

import appeer.general.log as _log
import appeer.general.utils as _utils

def parse_general_report(job, add_status_info=False):
    """
    Return a formatted report containing general information of a parse job

    Parameters
    ----------
    job : appeer.parse.parse_job.ParseJob
        appeer parse job
    add_status_info : bool
        If True, job status, succ./tot. and commit status are added

    Returns
    -------
    report : str
        General information of a parse job

    """

    report = ""

    report += _log.boxed_message(f'PARSE JOB: {job.label}', centered=True) + '\n'
    report += _log.center(f'Created on {_utils.human_datetime(job.date)}') + '\n'
    report += _log.center(job.description) + '\n\n'

    msg = ''

    msg += f'{"Log":19} {job.log}\n'
    msg += f'{"Parse directory":19} {job.parse_directory}\n'
    msg += f'{"Mode":19} {job.mode}'

    if add_status_info:
        msg += '\n'
        msg += f'{"Job status":19} {job.job_status}\n'
        msg += f'{"Succ./Tot.":19} {job.job_successes}/{job.no_of_publications}\n'
        msg += f'{"Committed":19} {job.job_parsed}'

    report += _log.boxed_message(msg) + '\n'

    return report
