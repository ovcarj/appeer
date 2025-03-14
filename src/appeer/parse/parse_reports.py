"""Generate appeer parse-related reports"""

from string import ascii_lowercase
from copy import deepcopy

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
        msg += f'{"Committed":19} {job.job_committed}'

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

def parse_job_summary(job):
    """
    Get a summary of a parse job

    Parameters
    ----------
    job : appeer.parse.parse_job.ParseJob
        appeer parse job

    Returns
    -------
    report : str
        Parse job summary

    """

    report = parse_general_report(job=job, add_status_info=True)

    if not job.actions:
        report += '\nNo publications added to the job.'

    else:

        report += '\n'
        report += action_list_summary(job.actions, add_committed_info=True)

    return report

def files_readability_report(files_readability):
    """
    Return a formatted report on whether a list of files is readable

    Parameters
    ----------
    files_readability : dict
        Dictionary of form {path1: True, path2: False, ...},
            where the keys are the absolute file paths and the values are
            True/False for readable/unreadable.

        Such a dictionary is generated by
            appeer.general.utils.file_list_readable

    Returns
    -------
    report : str
        A report on whether a list of files is readable

    """

    max_index = str(len(files_readability) - 1)
    paths = list(files_readability.keys())
    readable = ['T' if r else 'F' for r in files_readability.values()]

    max_index_len = max(len(max_index), len('Index'))
    max_path_len = max(len(max(paths, key=len)), len('Path'))
    max_r_len = len('Readable')

    _msg = f'{"Index":<{max_index_len}}  {"Path":<{max_path_len}}  {"Readable":^{max_r_len}}'

    report = _log.underlined_message(_msg)
    report += '\n'

    for i in range(len(files_readability)):
        report += f'{i:<{max_index_len}}  {paths[i]:<{max_path_len}}  {readable[i]:^{max_r_len}}\n'

    return report

def parse_start_report(job, run_parameters):
    """
    Return a formatted report at the beginning of a parse job run

    Parameters
    ----------
    job : appeer.parse.parse_job.ParseJob
        appeer parse job
    run_parameters : dict
        Dictionary containing restart_mode, cleanup,
            publishers, journals, data_types

    Returns
    -------
    report : str
        Report on the beginning of a parse job run

    """

    current_time = _utils.get_current_datetime()
    human_time = _utils.human_datetime(current_time)

    report = '\n'

    report += _log.boxed_message('PARSE JOB EXECUTION', centered=True)

    report += '\n'
    report += _log.center(f'{human_time}')
    report += '\n\n'

    msg = ''
    msg += f'{"no_of_publications":<20} {job.no_of_publications}\n'

    run_parameters_copy = deepcopy(run_parameters)

    if run_parameters_copy['publishers'] is None:
        run_parameters_copy['publishers'] = 'All'

    if run_parameters_copy['journals'] is None:
        run_parameters_copy['journals'] = 'All'

    if run_parameters_copy['data_types'] is None:
        run_parameters_copy['data_types'] = 'All'

    for name, value in run_parameters_copy.items():
        msg += f'{name:<20} {value}\n'

    msg = msg.rstrip('\n')

    report += _log.boxed_message(msg, header='Job parameters')

    report += '\n\n'

    if run_parameters['restart_mode'] == 'from_scratch':
        start_resume = 'Starting'

    else:
        start_resume = 'Resuming'

    report += _log.boxed_message(f'{start_resume} parsing job from step {job.job_step}/{job.no_of_publications - 1}')

    return report

def parse_step_report(job, action_index=None):
    """
    Return a formatted report at launching of a parse action

    Parameters
    ----------
    job : appeer.parse.parse_job.ParseJob
        appeer parse job
    action_index : int
        Index of the parse action that is beginning;
            defaults to ``job.job_step``

    Returns
    -------
    report : str
        Report on the launch of a parse action

    """

    if not action_index:
        action_index = job.job_step

    current_time = _utils.get_current_datetime()
    human_time = _utils.human_datetime(current_time)

    report = _log.boxed_message(f'Parsing entry {job.job_step}/{job.no_of_publications - 1}; {human_time}')

    return report

def parse_action_start(action):
    """
    Return a formatted report at the beginning of a parse action

    Parameters
    ----------
    action : appeer.parse.parse_action.ParseAction
        appeer parse action

    Returns
    -------
    report : str
        Report on the beginning of a parse action

    """

    filepath = action.input_file
    parser = action.parser

    align = len(max('Filepath', 'Parser', key=len)) + 2

    report = '\n'

    report += f'{"Filepath":<{align}} {filepath}\n'
    report += f'{"Parser":<{align}} {parser}\n'

    return report

def parsing_report(parser):
    """
    Return a formatted report on parsing a single file

    Parameters
    ----------
    parser : appeer.parse.parsers.<PUBLISHER>.<PUBLISHER>_<JOURNAL>_<DTYPE>
        An instance of an appeer parser

    Returns
    -------
    report : str
        A formatted report on parsing a single file

    """

    report = ''

    if parser.success:
        report += 'Parsing successful!\n\n'

    else:
        report += 'Parsing failed.\n'

    align = len(max(parser.metadata_list, key=len)) + 4

    for meta in parser.metadata_list:

        meta_brackets = f'[{meta.upper()}]'

        if meta != 'affiliations':
            report += f'{meta_brackets:<{align}} {getattr(parser, meta)}\n'

    if 'affiliations' in parser.metadata_list:

        report += '\n[AFFILIATIONS]'

        if parser.affiliations:

            align = len(str(len(parser.affiliations))) + 3

            report += '\n\n'

            for i, aff_list in enumerate(parser.affiliations):

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

def parse_end(job):
    """
    Return a formatted report at the end of a parse job

    Parameters
    ----------
    job : appeer.parse.parse_job.ParseJob
        appeer parse job

    Returns
    -------
    report : str
        Report on the end of a parse job

    """

    report = ''

    report += _log.boxed_message('PARSE JOB EXECUTED', centered=True) + '\n'

    align = len(max('Successes', 'Fails', key=len)) + 2

    msg = ''

    msg += f'{"Succeeded":<{align}} {job.job_successes}/{job.no_of_publications}\n'
    msg += f'{"Failed":<{align}} {job.job_fails}/{job.no_of_publications}\n'

    msg = msg.rstrip('\n')
    report += _log.boxed_message(msg, centered=True)

    report += '\n'

    if job.job_fails > 0:

        report += _log.boxed_message('Failed parse actions')
        report += '\n'

        report += action_list_summary(action_list=job.failed_actions)

    return report

def action_list_summary(action_list, add_committed_info=False): #pylint:disable='too-many-locals'
    """
    Return a summary of a list of parse actions

    Parameters
    ----------
    action_list : list of appeer.parse.parse_job.ParseAction
        List of appeer parse actions
    add_parsed_info : bool
        If True, write action.committed values to summary

    Returns
    -------
    report : str
        Summary of a list of actions

    """

    action_indices = [str(action.action_index) for action in action_list]
    inputs = [action.input_file for action in action_list]
    parsers = [action.parser or 'None' for action in action_list]
    statuses = [action.status for action in action_list]

    if add_committed_info:
        committed = [action.committed for action in action_list]

    max_index_len = max(len(max(action_indices, key=len)), len('Index'))
    max_input_len = max(len(max(inputs, key=len)), len('Filepath'))
    max_parser_len = max(len(max(parsers, key=len)), len('Parser'))
    max_status_len = max(len(max(statuses, key=len)), len('Status'))

    if add_committed_info:
        max_committed_len = max(len(max(committed, key=len)), len('Committed'))

    _msg = f'{"Index":<{max_index_len}}  {"Filepath":<{max_input_len}}  {"Parser":<{max_parser_len}}  {"Status":<{max_status_len}}'

    if add_committed_info:
        _msg += f'  {"Committed":<{max_committed_len}}'

    report = _log.underlined_message(_msg) + '\n'

    for i in range(len(action_list)):
        report += f'{action_indices[i]:<{max_index_len}}  {inputs[i]:<{max_input_len}}  {parsers[i]:<{max_parser_len}}    {statuses[i]:<{max_status_len}}'

        if add_committed_info:
            report += f'    {committed[i]:<{max_committed_len}}'

        report += '\n'

    return report

def uncommitted_parses(parses):
    """
    A summary of all uncommitted parse actions

    Parameters
    ----------
    parses : appeer.db.tables.parses.Parses
        Parses table

    Returns
    -------
    report : str
        Summary of all uncommitted parse actions

    """

    if parses.uncommitted:

        labels = [parse.label
                for parse in parses.uncommitted]
        parsers = [parse.parser
                for parse in parses.uncommitted]
        action_indices = [str(parse.action_index)
                for parse in parses.uncommitted]
        filepaths = [parse.input_file
                for parse in parses.uncommitted]

        max_label_len = max(len(max(labels, key=len)), len('Label'))
        max_index_len = max(len(max(action_indices, key=len)), len('Index'))
        max_parser_len = max(len(max(parsers, key=len)), len('Parser'))
        max_filepath_len = max(len(max(filepaths, key=len)), len('Filepath'))

        _msg = f'{"Label":<{max_label_len}}  {"Index":<{max_index_len}}  {"Parser":<{max_parser_len}}  {"Filepath":<{max_filepath_len}}'

        report = _log.underlined_message(_msg) + '\n'

        for i in range(len(parses.uncommitted)):
            report += f'{labels[i]:<{max_label_len}}  {action_indices[i]:<{max_index_len}}  {parsers[i]:<{max_parser_len}}  {filepaths[i]:<{max_filepath_len}}\n'

        report = report.rstrip('\n')

    else:
        report = 'No uncommitted parse entries found.'

    return report

def end_logo(job):
    """
    Write a logo and the parse job label at the end of the job

    Parameters
    ----------
    job : appeer.parse.parse_job.ParseJob
        appeer parse job

    Returns
    -------
    report : str
        Logo and scrape job label for the end of a scrape job

    """

    current_time = _utils.get_current_datetime()
    human_time = _utils.human_datetime(current_time)

    report = _log.get_logo(centered=True) + '\n'
    report += _log.boxed_message(f'PARSE JOB: {job.label}', centered=True) + '\n'
    report += _log.center(f'Finished on {human_time}') + '\n'

    return report

def parse_jobs_execution_report(parse_jobs_execution_dict):
    """
    Return a formatted report on whether a list of parse jobs are executed

    Parameters
    ----------
    parse_jobs_executed_dict : dict
        Dictionary of form {label1: status1, label2: status2, ...},
            where the keys are parse job labels and the values are
            the job statuses

        Such a dictionary is generated by
            appeer.parse.parse_scripts.get_execution_dict

    Returns
    -------
    report : str
        A report on whether a list of parse jobs exist and are executed

    """

    max_index = str(len(parse_jobs_execution_dict) - 1)
    labels = list(parse_jobs_execution_dict.keys())
    statuses = list(parse_jobs_execution_dict.values())

    max_index_len = max(len(max_index), len('Index'))
    max_label_len = max(len(max(labels, key=len)), len('Label'))
    max_statuses_len = len('Status')

    _msg = f'{"Index":<{max_index_len}}  {"Label":<{max_label_len}}  {"Status":^{max_statuses_len}}'

    report = _log.underlined_message(_msg)
    report += '\n'

    for i in range(len(parse_jobs_execution_dict)):
        report += f'{i:<{max_index_len}}  {labels[i]:<{max_label_len}}  {statuses[i]:^{max_statuses_len}}\n'

    return report
