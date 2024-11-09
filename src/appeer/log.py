import sys
import os
import logging
import click

import appeer.utils
from appeer import __version__

def init_logger(logname='appeer', logdir=None):
    """
    Initialize the logger object.

    Parameters
    ----------
    logname : str
        Name of the logger object (also used in naming in the log file)
    logdir : str
        Directory in which to store the log file. If not given, default to current directory

    Returns
    ----------
    logger : logging.Logger
        logging.Logger object

    """

    if logdir is None:
        logdir = os.getcwd()

    os.makedirs(logdir, exist_ok=True)

    if not logname.endswith('.log'):
        logname += '.log'

    logger = logging.getLogger(logname)

    logger.setLevel(logging.INFO) 

    stream_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(os.path.join(logdir, logname))
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    return logger

def get_logger_fh_path(logger):
    """
    Get path to where a log is stored 
    (baseFilename of the logger FileHandler with level INFO).

    Parameters
    ----------
    logger : logging.Logger
        logging.Logger object

    Returns
    ----------
    base_filename : str
        Path to where the log is stored.

    """

    if logger.hasHandlers():

        handlers = logger.handlers

        base_filename = ''

        for handler in handlers:

            if type(handler) == logging.FileHandler:
                if handler.level == 20:
                    base_filename += handler.baseFilename

        if base_filename == '':
            return 'No logger file handlers found'

        else:
            return base_filename

    else:
        return 'No logger handlers found.'

def appeer_start(start_datetime, logpath=None):
    """
    Report on the beginning of ``appeer`` execution.

    Parameters
    ----------
    start_datetime : str
        Starting datetime in ``%Y%m%d-%H%M%S`` format
    logpath : str | None
        Path to the logfile

    Returns
    ----------
    start_report : str
        Report on the beginning of ``appeer`` execution
 
    """

    start_report = ''

    log_dashes = get_log_dashes()
    logo = get_logo()

    start_report += logo + '\n'
    start_report += log_dashes + '\n'
    start_report += f'appeer started on {start_datetime}\n'
    start_report += log_dashes 

    if logpath:

        start_report += '\n'
        start_report += f'Logfile: {logpath}\n'
        start_report += log_dashes

    return start_report

def appeer_end(start_datetime):
    """
    Report on the end of ``appeer`` execution.

    Parameters
    ----------
    start_datetime : str
        Starting datetime in ``%Y%m%d-%H%M%S`` format

    Returns
    ----------
    end_report : str
        Report on the end of ``appeer`` execution
 
    """

    end_datetime = appeer.utils.get_current_datetime()

    end_report = ''

    log_dashes = get_log_dashes()

    runtime = appeer.utils.get_runtime(
            appeer.utils.convert_time_string(start_datetime),
            appeer.utils.convert_time_string(end_datetime)
            )

    end_report += log_dashes + '\n'
    end_report += f'appeer finished on {end_datetime}\n'
    end_report += f'Total runtime: {runtime}'

    return end_report

def get_log_dashes():
    """
    Create some dashes for logging.

    Returns
    -------
    str
        Dashes for logging

    """

    return '–' * 50

def get_short_log_dashes():
    """
    Create some dashes for logging.

    Returns
    -------
    str
        Dashes for logging

    """

    return '–' * 25

def get_very_short_log_dashes():
    """
    Create some dashes for logging.

    Returns
    -------
    str
        Dashes for logging

    """

    return '–' * 10

def boxed_message(message):
    """
    Create a box around a message.

    Returns
    -------
    boxed_message : str
        Message with a box around it

    """

    dashes = '—' * (len(message) + 4)

    boxed_message = f'{dashes}\n| {message} |\n{dashes}'

    return boxed_message
        
def get_logo():
    """
    Create the ``appeer`` logo.

    Returns
    -------
    str
        The ``appeer`` logo

    """

    logo = rf"""
#                                         
#    __ _  _ __   _ __    ___   ___  _ __ 
#   / _` || '_ \ | '_ \  / _ \ / _ \| '__|
#  | (_| || |_) || |_) ||  __/|  __/| |   
#   \__,_|| .__/ | .__/  \___| \___||_|   
#         | |    | |                      
#         |_|    |_|    v={__version__}                  
#
#
"""

    return logo

def ask_yes_no(question):
    """
    Forces the user to type 'Y' or 'n' and returns the answer.

    Parameters
    ----------
    question : str
        Question to ask the user

    Returns
    -------
    answer : str
        'Y' or 'n'

    """

    dashes = get_log_dashes()

    input_ok = False

    while(input_ok == False):

        answer = input(question)

        click.echo(dashes)

        try:
            assert (answer == 'Y' or answer == 'n')
            input_ok = True

        except AssertionError:
            click.echo('Please enter "Y" or "n".')
            click.echo(dashes)

    return answer
