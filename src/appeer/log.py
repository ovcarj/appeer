import sys
import os
import logging

import appeer.utils

def init_logger(start_time=None, logdir=None, logname='appeer'):
    """
    Initialize the logger object.

    Parameters
    ----------
    logdir : str
        Directory in which to store the log file. If not given, default to current directory
    start_time : str
        Current datetime in ``%Y%m%d-%H%M%S`` format (used in naming the log file). If not given, default to current time
    logname : str
        Name of the logger object (also used in naming in the log file)

    Returns
    ----------
    logger : logging.Logger
        logging.Logger object

    """

    if start_time is None:
        start_time = appeer.utils.get_current_datetime()

    if logdir is None:
        logdir = os.getcwd()

    os.makedirs(logdir, exist_ok=True)

    if not logdir.endswith('/'):
        logdir += '/'

    logger = logging.getLogger(logname)
    logger.setLevel(logging.INFO) 
    stream_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stream_handler)
    file_handler = logging.FileHandler(f'{logdir}{logname}_{start_time}.log')
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    return logger

def get_log_dashes():
    """
    Create some dashes for logging.

    Returns
    -------
    str
        Dashes for logging

    """

    return '------------------------------------'

def get_logo():
    """
    Create the ``appeer`` logo.

    Returns
    -------
    str
        The ``appeer`` logo

    """

    logo = r"""
#                                         
#                                         
#    __ _  _ __   _ __    ___   ___  _ __ 
#   / _` || '_ \ | '_ \  / _ \ / _ \| '__|
#  | (_| || |_) || |_) ||  __/|  __/| |   
#   \__,_|| .__/ | .__/  \___| \___||_|   
#         | |    | |                      
#         |_|    |_|    v=0.0.1                  
#
#
"""

    return logo
