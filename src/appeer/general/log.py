"""Creates and writes log files"""

import sys
import os
import logging
import click

from appeer.general import utils

from appeer import __version__


def init_logger(log_path, log_name=None):
    """
    Initialize the logger object

    Parameters
    ----------
    log_path : str
        Path to the log file
    log_name : str
        Optional name of the logger object

    Returns
    ----------
    logger : logging.Logger
        logging.Logger object

    """

    if not log_path.endswith('.log'):
        log_path += '.log'

    log_dir = os.path.dirname(log_path)

    if not utils.directory_exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    if log_name:
        logger = logging.getLogger(log_name)

    else:
        logger = logging.getLogger(__name__)

    logger.setLevel(logging.INFO)

    if not logger.hasHandlers():

        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)

        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)

    return logger

def get_logger_fh_path(logger):
    """
    Get path to where a log is stored 
        (baseFilename of the logger FileHandler with level INFO)

    Parameters
    ----------
    logger : logging.Logger
        logging.Logger object

    Returns
    -------
    base_filename : str
        Path to where the log is stored.

    """

    if logger.hasHandlers():

        handlers = logger.handlers

        base_filename = ''

        for handler in handlers:

            if isinstance(handler, logging.FileHandler):
                if handler.level == 20:
                    base_filename += handler.baseFilename

        if base_filename == '':
            return 'No logger file handlers found'

        return base_filename

    return 'No logger handlers found.'

def appeer_start(start_datetime, log_path=None):
    """
    Report on the beginning of ``appeer`` execution

    Parameters
    ----------
    start_datetime : str
        Starting datetime in ``%Y%m%d-%H%M%S`` format
    log_path : str | None
        Path to the logfile

    Returns
    -------
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

    if log_path:

        start_report += '\n'
        start_report += f'Logfile: {log_path}\n'
        start_report += log_dashes

    return start_report

def appeer_end(start_datetime):
    """
    Report on the end of ``appeer`` execution

    Parameters
    ----------
    start_datetime : str
        Starting datetime in ``%Y%m%d-%H%M%S`` format

    Returns
    ----------
    end_report : str
        Report on the end of ``appeer`` execution
 
    """

    end_datetime = utils.get_current_datetime()

    end_report = ''

    log_dashes = get_log_dashes()

    runtime = utils.get_runtime(
            utils.convert_time_string(start_datetime),
            utils.convert_time_string(end_datetime)
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
    Create some dashes for logging

    Returns
    -------
    str
        Dashes for logging

    """

    return '–' * 25

def get_very_short_log_dashes():
    """
    Create some dashes for logging

    Returns
    -------
    str
        Dashes for logging

    """

    return '–' * 10

def center(message, width=None):
    """
    Center a message

    Parameters
    ----------
    message : str
        A message to be centered
    width : int
        Width of the line for centering

    Returns
    -------
    centered : str
        Centered message

    """

    if not width:
        width = 80

    centered = ''

    lines = message.split('\n')

    if len(lines) > 1:

        for line in lines:
            centered += line.center(width) + '\n'

        centered = centered.rstrip('\n')

    else:
        centered = message.center(width)

    return centered

def boxed_message(message, centered=False, width=None, header=None):
    """
    Create a box around a message

    Parameters
    ----------
    message : str
        A string to be written in a box
    center : bool
        If given, the message will be centered using str.center(width=width)
    width : int
        Width of the line for centering
    header : str
        If given, write a header above the box

    Returns
    -------
    box_message : str
        Message with a box around it

    """

    box_message = ''

    lines = message.split('\n')

    longest_line_length = len(max(lines, key=len))
    dashes = '+' + '—' * (longest_line_length + 2) + '+'

    if header:
        box_message += header.center(len(dashes)) + '\n'

    box_message += dashes + '\n'

    for line in lines:
        box_message += f'| {line:<{longest_line_length}} |\n'

    box_message += dashes

    if centered:
        box_message = center(box_message, width=width)

    return box_message

def outlined_message(message):
    """
    Create a message with dashed lines surrounding it

    Parameters
    ----------
    message : str
        A string to be surrounded by dashed lines

    Returns
    -------
    outlined : str
        A message surrounded by dashed lines

    """

    dashes = '–'* len(message)

    outlined = dashes
    outlined += '\n'
    outlined += message
    outlined += '\n'
    outlined += dashes

    return outlined

def underlined_message(message):
    """
    Create a message with a dashed line below it

    Parameters
    ----------
    message : str
        A string to be underlined by a dashed line

    Returns
    -------
    underlined : str
        A message underlined by a dashed line

    """

    dashes = '–'* len(message)

    underlined = message
    underlined += '\n'
    underlined += dashes

    return underlined

def get_logo(centered=True, width=None):
    """
    Create the ``appeer`` logo

    Parameters
    ----------
    center : bool
        If given, the logo will be centered using str.center(width=width)
    width : int
        Width of the line for centering

    Returns
    -------
    str
        The ``appeer`` logo

    """

    logo = rf"""
+-------------------------------------------+
|    __ _  _ __   _ __    ___   ___  _ __   |
|   / _` || '_ \ | '_ \  / _ \ / _ \| '__|  |
|  | (_| || |_) || |_) ||  __/|  __/| |     |
|   \__,_|| .__/ | .__/  \___| \___||_|     |
|         | |    | |                        |
|         |_|    |_|     >>> v={__version__}        |
|                                           |
+-------------------------------------------+
"""

    if centered:
        logo = center(logo, width=width)

    return logo

def ask_yes_no(question):
    """
    Forces the user to type 'Y' or 'n' and returns the answer

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

    while input_ok is False:

        answer = input(question)

        click.echo(dashes)

        try:
            assert answer in ('Y', 'n')
            input_ok = True

        except AssertionError:
            click.echo('Please enter "Y" or "n".')
            click.echo(dashes)

    return answer
