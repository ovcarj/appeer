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

def scrape_general_report(job, add_status_info=False):
    """
    Return a formatted report containing general information of a scrape job

    Parameters
    ----------
    job : appeer.scrape.scrape_job.ScrapeJob
        appeer scrape job
    add_status_info : bool
        If True, job status, succ./tot. and parsed status are added

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

    if add_status_info:
        msg += '\n'
        msg += f'{"Job status":19} {job.job_status}\n'
        msg += f'{"Succ./Tot.":19} {job.job_successes}/{job.no_of_publications}\n'
        msg += f'{"Parsed":19} {job.job_parsed}'

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

def action_list_summary(action_list, add_parsed_info=False): #pylint:disable='too-many-locals'
    """
    Return a summary of a list of scrape actions

    Parameters
    ----------
    action_list : list of appeer.scrape.scrape_job.ScrapeAction
        List of appeer scrape actions
    add_parsed_info : bool
        If True, write action.parsed values to summary

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

    if add_parsed_info:
        parsed = [action.parsed for action in action_list]

    max_index_len = max(len(max(action_indices, key=len)), len('Index'))
    max_url_len = max(len(max(urls, key=len)), len('URL'))
    max_journal_len = max(len(max(journals, key=len)), len('Journal'))
    max_strategy_len = max(len(max(strategies, key=len)), len('Strategy'))
    max_status_len = max(len(max(statuses, key=len)), len('Status'))

    if add_parsed_info:
        max_parsed_len = max(len(max(parsed, key=len)), len('Parsed'))

    _msg = f'{"Index":<{max_index_len}}  {"URL":<{max_url_len}}  {"Journal":<{max_journal_len}}  {"Strategy":<{max_strategy_len}}  {"Status":<{max_status_len}}'

    if add_parsed_info:
        _msg += f'  {"Parsed":<{max_parsed_len}}'

    report = _log.underlined_message(_msg) + '\n'

    for i in range(len(action_list)):
        report += f'{action_indices[i]:<{max_index_len}}  {urls[i]:<{max_url_len}}  {journals[i]:<{max_journal_len}}  {strategies[i]:<{max_strategy_len}}    {statuses[i]:<{max_status_len}}'

        if add_parsed_info:
            report += f'  {parsed[i]:<{max_parsed_len}}'

        report += '\n'

    return report

def end_logo(job):
    """
    Write a logo and the scrape job label at the end of the job

    Parameters
    ----------
    job : appeer.scrape.scrape_job.ScrapeJob
        appeer scrape job

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

def scrape_jobs_summary(scrape_table):
    """
    Summary of all scrape jobs in the database

    Parameters
    ----------
    scrape_table : appeer.db.tables.scrape_jobs.ScrapeJobs
        Instance of ScrapeJobs table

    Returns
    -------
    report : str
        Summary of all scrape jobs

    """

    _msg = f'{"Label":<30} {"Description":<35} {"S":^4} {"P":^4} {"Succ./Tot.":^10}'
    header_length = len(_msg)
    dashes = header_length * '–'

    report = f'{dashes}\n{_msg}\n{dashes}\n'

    for job in scrape_table.entries:

        description = job.description

        if len(description) > 30:
            description = description[0:30] + '...'

        succ_tot = f'{job.job_successes}/{job.no_of_publications}'

        report += f'{job.label:<30} {description:<35} {job.job_status:^4} {job.job_parsed:^4} {succ_tot:^10}' + '\n'

    report += dashes + '\n'

    report += 'S = Job status: (I) Initialized; (W) Waiting; (R) Running; (X) Executed; (E) Error' + '\n'
    report += 'P = Job completely parsed: (T) True; (F) False' + '\n'
    report += 'Succ./Tot. = Ratio of successful scrapes over total inputted URLs' + '\n'

    report += dashes

    return report

def scrape_job_summary(job):
    """
    Get a summary of a scrape job

    Parameters
    ----------
    job : appeer.scrape.scrape_job.ScrapeJob
        appeer scrape job

    Returns
    -------
    report : str
        Scrape job summary

    """

    report = scrape_general_report(job=job, add_status_info=True)

    if not job.actions:
        report += '\nNo publications added to the job.'

    else:

        report += '\n'
        report += action_list_summary(job.actions, add_parsed_info=True)

    return report

def unparsed_scrapes(scrapes):
    """
    A summary of all unparsed scrape actions

    Parameters
    ----------
    scrapes : appeer.db.tables.scrapes.Scrapes
        Scrapes table

    Returns
    -------
    report : str
        Summary of all unparsed scrape actions

    """

    if scrapes.unparsed:

        labels = [scrape.label
                for scrape in scrapes.unparsed]
        action_indices = [str(scrape.action_index)
                for scrape in scrapes.unparsed]
        urls = [scrape.url
                for scrape in scrapes.unparsed]

        max_label_len = max(len(max(labels, key=len)), len('Label'))
        max_index_len = max(len(max(action_indices, key=len)), len('Index'))
        max_url_len = max(len(max(urls, key=len)), len('URL'))

        _msg = f'{"Label":<{max_label_len}}    {"Index":<{max_index_len}}    {"URL":<{max_url_len}}'

        report = _log.underlined_message(_msg) + '\n'

        for i in range(len(scrapes.unparsed)):
            report += f'{labels[i]:<{max_label_len}}    {action_indices[i]:<{max_index_len}}    {urls[i]:<{max_url_len}}\n'

        report = report.rstrip('\n')

    else:
        report = 'No unparsed scrapes found.'

    return report

def scrape_jobs_execution_report(scrape_jobs_execution_dict):
    """
    Return a formatted report on whether a list of scrape jobs are executed

    Parameters
    ----------
    scrape_jobs_executed_dict : dict
        Dictionary of form {label1: status1, label2: status2, ...},
            where the keys are scrape job labels and the values are
            the job statuses

        Such a dictionary is generated by
            appeer.scrape.scrape_scripts.get_execution_dict

    Returns
    -------
    report : str
        A report on whether a list of scrape jobs exist and are executed

    """

    max_index = str(len(scrape_jobs_execution_dict) - 1)
    labels = list(scrape_jobs_execution_dict.keys())
    statuses = list(scrape_jobs_execution_dict.values())

    max_index_len = max(len(max_index), len('Index'))
    max_label_len = max(len(max(labels, key=len)), len('Label'))
    max_statuses_len = len('Status')

    _msg = f'{"Index":<{max_index_len}}  {"Label":<{max_label_len}}  {"Status":^{max_statuses_len}}'

    report = _log.underlined_message(_msg)
    report += '\n'

    for i in range(len(scrape_jobs_execution_dict)):
        report += f'{i:<{max_index_len}}  {labels[i]:<{max_label_len}}  {statuses[i]:^{max_statuses_len}}\n'

    return report

def scrape_output_files_report(action_files):
    """
    Return a formatted report on whether scraped files are directly accessible

    Parameters
    ----------
    action_files : list of _ScrapeActionFile
        List of _ScrapeActionFile named tuples,
            containing label, action_index, out_file and file_ok fields.

            Such a list of named tuples is generated by
                appeer.scrape.scrape_scripts.check_action_outputs

    Returns
    -------
    report : str
        A report on whether scraped files are directly accessible

    """

    labels = [action.label for action in action_files]
    action_indices = [str(action.action_index) for action in action_files]
    out_files = [action.out_file for action in action_files]
    readable = ['T' if action.file_ok else 'F' for action in action_files]

    max_label_len = max(len(max(labels, key=len)), len('Label'))
    max_index_len = max(len(max(action_indices, key=len)),
            len('Action index'))
    max_out_len = max(len(max(out_files, key=len)), len('Download'))
    max_r_len = len('Readable')

    _msg = f'{"Label":<{max_label_len}}  {"Action_index":<{max_index_len}}  {"Download":<{max_out_len}}  {"Readable":^{max_r_len}}'

    report = _log.underlined_message(_msg)
    report += '\n'

    for i in range(len(action_files)):
        report += f'{labels[i]:<{max_label_len}}  {action_indices[i]:<{max_index_len}}  {out_files[i]:<{max_out_len}}  {readable[i]:^{max_r_len}}\n'

    return report
