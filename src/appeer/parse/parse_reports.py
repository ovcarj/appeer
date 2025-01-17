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

def parse_jobs_summary(parse_table):
    """
    Summary of all parse jobs in the database

    Parameters
    ----------
    scrape_table : appeer.db.tables.parse_jobs.ParseJobs
        Instance of ParseJobs table

    Returns
    -------
    report : str
        Summary of all parse jobs

    """

    _msg = f'{"Label":<30} {"Description":<25} {"M":^4} {"S":^4} {"C":^4} {"Succ./Tot.":^10}'

    header_length = len(_msg)
    dashes = header_length * '–'

    report = f'{dashes}\n{_msg}\n{dashes}\n'

    for job in parse_table.entries:

        description = job.description

        if len(description) > 20:
            description = description[0:20] + '...'

        succ_tot = f'{job.job_successes}/{job.no_of_publications}'

        report += f'{job.label:<30} {description:<25} {job.mode:^4} {job.job_status:^4} {job.job_committed:^4} {succ_tot:^10}' + '\n'

    report += dashes + '\n'

    report += 'M = Parse mode: (A) Auto; (E) Everything; (S) Scrape job; (F) File list' + '\n'
    report += 'S = Job status: (I) Initialized; (R) Running; (X) Executed/Finished; (E) Error' + '\n'
    report += 'C = Job committed: (T) True; (F) False' + '\n'
    report += 'Succ./Tot. = Ratio of successfully parsed over total inputted publications' + '\n'

    report += dashes

    return report
