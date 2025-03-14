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

def commit_start_report(job, run_parameters):
    """
    Return a formatted report at the beginning of commit job execution

    Parameters
    ----------
    job : appeer.commit.commit_job.CommitJob
        appeer commit job
    run_parameters : dict
        Dictionary containing restart_mode, no_parse_mark, overwrite

    Returns
    -------
    report : str
        Report on the beginning of commit job execution

    """

    current_time = _utils.get_current_datetime()
    human_time = _utils.human_datetime(current_time)

    report = '\n'

    report += _log.boxed_message('COMMIT JOB EXECUTION', centered=True)

    report += '\n'
    report += _log.center(f'{human_time}')
    report += '\n\n'

    msg = ''
    msg += f'{"no_of_publications":<20} {job.no_of_publications}\n'

    for name, value in run_parameters.items():
        msg += f'{name:<20} {value}\n'

    msg = msg.rstrip('\n')

    report += _log.boxed_message(msg, header='Job parameters')

    report += '\n\n'

    if run_parameters['restart_mode'] == 'from_scratch':
        start_resume = 'Starting'

    else:
        start_resume = 'Resuming'

    report += _log.boxed_message(f'{start_resume} commit job from step {job.job_step}/{job.no_of_publications - 1}')

    return report
