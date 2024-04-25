import sys
import logging

def init_logger(start_time, logname='appeer'):
    """
    Initialize the logger object.

    Parameters
    ----------
    start_time : str
        Current datetime in ``%Y%m%d-%H%M%S`` format (used in naming the log file)
    logname : str
        Name of the logger object (used in naming in the log file)

    Returns
    ----------
    logger : logging.Logger
        logging.Logger object

    """

    logger = logging.getLogger(logname)
    logger.setLevel(logging.INFO) 
    stream_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stream_handler)
    file_handler = logging.FileHandler(f'{logname}_{start_time}.log')
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
