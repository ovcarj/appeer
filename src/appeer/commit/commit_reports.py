"""Generate appeer commit-related reports"""

from string import ascii_lowercase

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

def commit_step_report(job, action_index=None):
    """
    Return a formatted report at launching of a commit action

    Parameters
    ----------
    job : appeer.commit.commit_job.CommitJob
        appeer commit job
    action_index : int
        Index of the commit action that is beginning;
            defaults to ``job.job_step``

    Returns
    -------
    report : str
        Report on the launch of a commit action

    """

    if not action_index:
        action_index = job.job_step

    current_time = _utils.get_current_datetime()
    human_time = _utils.human_datetime(current_time)

    report = _log.boxed_message(f'Committing entry {job.job_step}/{job.no_of_publications - 1}; {human_time}')

    return report

def commit_action_start(action):
    """
    Return a formatted report at the beginning of a commit action

    Parameters
    ----------
    action : appeer.commit.commit_action.CommitAction
        appeer commit action

    Returns
    -------
    report : str
        Report on the beginning of a commit action

    """

    metadata_list = [
            'doi',
            'publisher',
            'journal',
            'title',
            'publication_type',
            'affiliations',
            'received',
            'accepted',
            'published'
            ]

    report = '\n' + _log.underlined_message('METADATA SOURCE') + '\n'

    report += f'{"Parse job label":20} {action.parse_label}\n'
    report += f'{"Parse action index":20} {action.parse_action_index}\n\n'

    report += _log.underlined_message('METADATA')
    report += '\n\n'

    align = len(max(metadata_list, key=len)) + 4

    for meta in metadata_list:

        meta_brackets = f'[{meta.upper()}]'

        if meta != 'affiliations':
            report += f'{meta_brackets:<{align}} {getattr(action, meta)}\n'

    report += '\n[AFFILIATIONS]'

    if action.affiliations:

        affiliations = _utils.aff_str2list(action.affiliations)

        align = len(str(len(affiliations))) + 3

        report += '\n\n'

        for i, aff_list in enumerate(affiliations):

            if len(aff_list) > 1:
                suff = ascii_lowercase[:len(aff_list)]

            else:
                suff = ' '

            for j, aff in enumerate(aff_list):

                aff_string = f'({i+1}{suff[j].strip()})'

                report += f'{aff_string:<{align}} {aff}\n'

    else:
        report += f'{"":<{align}} None'

    return report

def commit_action_end(action, duplicate):
    """
    Return a formatted report at the end of a commit action

    Parameters
    ----------
    action : appeer.commit.commit_action.CommitAction
        appeer commit action

    Returns
    -------
    report : str
        Report on the beginning of a commit action
    duplicate : bool
        Whether the DOI was already in the database

    """

    report = _log.underlined_message('DATABASE STATUS UPDATE') + '\n'

    report += f'{"entry":18} {action.doi}\n'
    report += f'{"preexisting":18} {duplicate}\n'

    report += f'{"database_action":18} '

    passed = bool(action.passed == 'T')

    if not duplicate and passed:
        report += 'INSERT'

    elif duplicate and passed:
        report += 'REPLACE'

    elif duplicate and not passed:
        report += 'SKIP'

    elif not duplicate and not passed:
        report += 'FAIL'

    report += '\n'

    return report
