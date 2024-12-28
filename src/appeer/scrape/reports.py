"""Generate appeer scrape-related reports"""

import appeer.general.log as _log
import appeer.general.utils as _utils

def appeer_start(start_datetime=None):
    """
    Report on the beginning of ``appeer`` execution

    Parameters
    ----------
    start_datetime : str
        Starting datetime in ``%Y%m%d-%H%M%S`` format

    Returns
    ----------
    start_report : str
        Report on the beginning of ``appeer`` execution
 
    """

    if not start_datetime:
        start_datetime = _utils.get_current_datetime()

    human_time = _utils.human_datetime(start_datetime)

    start_report = ''

    logo = _log.get_logo()

    start_msg = f'appeer started on {human_time}'
    _dashes = '–' * len(start_msg)

    start_report += logo + '\n'
    start_report += _dashes + '\n'
    start_report += f'{start_msg}\n'
    start_report += _dashes

    return start_report

def scrape_general_report(job):
    """
    Return a formatted report containing general information of a scrape job

    Parameters
    ----------
    job : appeer.scrape.scrape_job.ScrapeJob
        appeer scrape job

    Returns
    -------
    report : str
        General information of a scrape job

    """

    report = ""

    report += _log.boxed_message(f'SCRAPE JOB: {job.label}', centered=True) + '\n'
    report += _log.center(f'Created on {_utils.human_datetime(job.date)}') + '\n'
    report += _log.center(job.description) + '\n\n'

    msg = ''

    msg += f'{"Log":19} {job.log}\n'
    msg += f'{"Download directory":19} {job.download_directory}\n'
    msg += f'{"Output ZIP file":19} {job.zip_file}'

    report += _log.boxed_message(msg) + '\n'

    return report

def scrape_strategy_report(plan, offset=0): #pylint:disable=too-many-locals
    """
    Create a strategy report for the given scrape plan

    Parameters
    ----------
    plan : appeer.scrape.strategies.scrape_plan.ScrapePlan
        Scrape plan
    offset : int
        Start counting URLs from ``offset``, useful if the
            scrape job contains preexisting publications

    Returns
    -------
    report : str
        Strategy report for the given scrape plan

    """

    report = ''

    report += f'{_log.boxed_message("SCRAPE PLAN", centered=True)}\n'

    no_of_urls = len(plan.url_list)

    msg = ''
    for journal, count in plan.journals_count.items():
        msg += f'{journal:<12} {count}/{no_of_urls}\n'

    msg = msg.rstrip('\n')
    report += _log.boxed_message(msg, header='Journal codes')

    report += '\n\n'

    msg = ''

    for strategy, count in plan.strategies_count.items():
        msg += f'{strategy:<12} {count}/{no_of_urls}\n'

    msg = msg.rstrip('\n')
    report += _log.boxed_message(msg, header='Planned strategies')

    report += '\n\n'

    max_index_len = max(len(str(len(plan.strategies) + offset)),
            len('Index'))
    max_url_len = max(len(max(plan.url_list, key=len)),
            len('URL'))
    max_journal_len = max(len(max(plan.journals_count.keys(), key=len)),
            len('Journal'))
    max_strat_len = max(len(max(plan.strategies_count.keys(), key=len)),
            len('Strategy'))

    report += _log.underlined_message(f'{"Index":<{max_index_len}}  {"URL":<{max_url_len}}  {"Journal":<{max_journal_len}}  {"Strategy"}')
    report += '\n'

    for i, strategy in plan.strategies.items():

        ind = str(int(i) + offset)

        url = strategy['url']
        jc = strategy['journal_code']
        sc = strategy['strategy_code']

        report += f'{ind:<{max_index_len}}  {url:<{max_url_len}}  {jc:<{max_journal_len}}  {sc:<{max_strat_len}}\n'

    return report

def scrape_start_report(job, run_parameters):
    """
    Return a formatted report at the beginning of a scrape job run

    Parameters
    ----------
    job : appeer.scrape.scrape_job.ScrapeJob
        appeer scrape job
    run_parameters : dict
        Dictionary containing scrape_mode, cleanup, sleep_time,
            max_tries, retry_sleep_time

    Returns
    -------
    report : str
        Report on the beginning of a scrape job run

    """

    current_time = _utils.get_current_datetime()
    human_time = _utils.human_datetime(current_time)

    report = '\n'

    report += _log.boxed_message('SCRAPE JOB EXECUTION', centered=True)

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

    if run_parameters['scrape_mode'] == 'from_scratch':
        start_resume = 'Starting'

    else:
        start_resume = 'Resuming'

    report += _log.boxed_message(f'{start_resume} scraping job from step {job.job_step}/{job.no_of_publications - 1}')

    return report

def scrape_step_report(job, action_index=None):
    """
    Return a formatted report at the beginning of a scrape action

    Parameters
    ----------
    job : appeer.scrape.scrape_job.ScrapeJob
        appeer scrape job
    action_index : int
        Index of the scrape action that is beginning;
            defaults to ``job.job_step``

    Returns
    -------
    report : str
        Report on the beginning of a scrape action

    """

    if not action_index:
        action_index = job.job_step

    current_time = _utils.get_current_datetime()
    human_time = _utils.human_datetime(current_time)

    report = _log.boxed_message(f'Scraping entry {job.job_step}/{job.no_of_publications - 1}; {human_time}')

    return report

def scrape_action_start(action):
    """
    Return a formatted report at the beginning of a scrape action

    Parameters
    ----------
    job : appeer.scrape.scrape_action.ScrapeAction
        appeer scrape action

    Returns
    -------
    report : str
        Report on the beginning of a scrape action

    """

    url = action.url
    journal = action.journal
    strategy = action.strategy
    method = action.method

    align = len(max('URL', 'Journal', 'Strategy', 'Method', key=len)) + 2

    report = ''

    report += f'{"URL":<{align}} {url}\n'
    report += f'{"Journal":<{align}} {journal}\n'
    report += f'{"Strategy":<{align}} {strategy}\n'
    report += f'{"Method":<{align}} {method}\n'

    return report

def scrape_action_end(action):
    """
    Return a formatted report at the end of a scrape action

    Parameters
    ----------
    job : appeer.scrape.scrape_action.ScrapeAction
        appeer scrape action

    Returns
    -------
    report : str
        Report on the beginning of a scrape action

    """

    if action.success == 'F':
        success = 'False'

    else:
        success = 'True'

    out_file = action.out_file

    align = len(max('Success', 'OutputFile', key=len)) + 2

    report = _log.underlined_message(f'Scrape #{action.action_index} End')
    report += '\n'

    report += f'{"Success":<{align}} {success}\n'
    report += f'{"Download":<{align}} {out_file}\n'

    return report

def requests_report(request):
    """
    Return a formatted report on Request status

    Parameters
    ----------
    request : appeer.scrape.requests.Request
        appeer request

    Returns
    -------
    report : str
        Request status report

    """

    align = len(max('Success', 'Error', 'StatusCode', key=len)) + 2

    if request.success:
        success = 'True'
    else:
        success = 'False'

    if not request.error:
        error = 'None'
    else:
        error = request.error

    if not request.status:
        status = 'None'
    else:
        status = request.status

    report = f'{"Success":<{align}} {success}\n'
    report += f'{"Error":<{align}} {error}\n'
    report += f'{"StatusCode":<{align}} {status}\n'

    return report

def doi_report(_doi_report):
    """
    Return a formatted report on the result of _doi_handler()

    Parameters
    ----------
    doi_report : dict
        Dictionary containing
            'success', 'resolved_url', 'resolved_journal', 'new_strategy'

    Returns
    -------
    report : str
        Report on the result of _doi_handler()

    """

    align = len(max('Success', 'Resolved_URL', 'Resolved_Journal',
        'New_Strategy', key=len)) + 2

    report = _log.underlined_message('DOI Resolution') + '\n'

    report += f'{"Success":<{align}} {_doi_report["success"]}\n'
    report += f'{"Resolved_URL":<{align}} {_doi_report["resolved_url"]}\n'
    report += f'{"Resolved_Journal":<{align}} {_doi_report["resolved_journal"]}\n'
    report += f'{"New_Strategy":<{align}} {_doi_report["new_strategy"]}\n'

    return report

def scrape_end(job):
    """
    Return a formatted report at the end of a scrape job

    Parameters
    ----------
    job : appeer.scrape.scrape_job.ScrapeJob
        appeer scrape action

    Returns
    -------
    report : str
        Report on the beginning of a scrape action

    """

    report = ''

    report += _log.boxed_message('SCRAPE JOB EXECUTED', centered=True) + '\n'

    align = len(max('Successes', 'Fails', key=len)) + 2

    msg = ''

    msg += f'{"Succeeded":<{align}} {job.job_successes}/{job.no_of_publications}\n'
    msg += f'{"Failed":<{align}} {job.job_fails}/{job.no_of_publications}\n'

    msg = msg.rstrip('\n')
    report += _log.boxed_message(msg, centered=True)

    report += '\n'

    if job.job_fails > 0:

        report += _log.boxed_message('Failed URLS')
        report += '\n'

        report += action_list_summary(action_list=job.failed_actions)

    return report

def action_list_summary(action_list):
    """
    Return a summary of a list of scrape actions

    Parameters
    ----------
    action_list : list of appeer.scrape.scrape_job.ScrapeAction
        List of appeer scrape actions

    Returns
    -------
    report : str
        Summary of a list of actions

    """

    action_indices = [str(action.action_index) for action in action_list]
    urls = [action.url for action in action_list]
    journals = [action.journal for action in action_list]
    strategies = [action.strategy for action in action_list]
    statuses = [action.status for action in action_list]

    max_index_len = max(len(max(action_indices, key=len)), len('Index'))
    max_url_len = max(len(max(urls, key=len)), len('URL'))
    max_journal_len = max(len(max(journals, key=len)), len('Journal'))
    max_strategy_len = max(len(max(strategies, key=len)), len('Strategy'))
    max_status_len = max(len(max(statuses, key=len)), len('Status'))

    report = _log.underlined_message(f'{"Index":<{max_index_len}}  {"URL":<{max_url_len}}  {"Journal":<{max_journal_len}}  {"Strategy":<{max_strategy_len}}  {"Status":<{max_status_len}}') + '\n'

    for i in range(len(action_list)):
        report += f'{action_indices[i]:<{max_index_len}}  {urls[i]:<{max_url_len}}  {journals[i]:<{max_journal_len}}  {strategies[i]:<{max_strategy_len}}    {statuses[i]:<{max_status_len}}\n'

    return report

def end_logo(job):
    """
    Write a logo and the scrape job label at the end of the job

    Parameters
    ----------
    job : appeer.scrape.scrape_job.ScrapeJob
        appeer scrape action

    Returns
    -------
    report : str
        Logo and scrape job label for the end of a scrape job

    """

    current_time = _utils.get_current_datetime()
    human_time = _utils.human_datetime(current_time)

    report = _log.get_logo(centered=True) + '\n'
    report += _log.boxed_message(f'SCRAPE JOB: {job.label}', centered=True) + '\n'
    report += _log.center(f'Finished on {human_time}') + '\n'

    return report