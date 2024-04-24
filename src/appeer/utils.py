import sys
import time
import json
import logging

from shutil import make_archive, rmtree
from datetime import datetime

def _init_logger(start_time, logname='appeer'):
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

def load_json(json_filename):
    """
    Load a JSON file to a list of dictionaries.

    Parameters
    ----------
    json_filename : int
        Input JSON filename

    Returns
    -------
    list
        List of dictionaries generated from the JSON file.

    """

    with open(json_filename, encoding='utf-8-sig') as f:
        data = json.load(f)

    return data

def write_text_to_file(path_to_file, text_data):
    """
    Write text to a file.

    Parameters
    ----------
    path_to_file : str
        Output file path
    text_data : str
        Text data to be written into a file

    """

    with open(path_to_file, 'w+') as f:
        f.write(text_data)

def get_current_datetime():
    """
    Get current datetime in the ``%Y%m%d-%H%M%S`` format.

    Returns
    -------
    str
        Current datetime in ``%Y%m%d-%H%M%S`` format.

    """

    timestr = time.strftime("%Y%m%d-%H%M%S")

    return timestr

def convert_time_string(time_string):
    """
    Convert datetime from a string in ``%Y%m%d-%H%M%S`` format to a ``datetime.datetime`` object.

    Parameters
    ----------
    time_string : str
        Datetime string in ``%Y%m%d-%H%M%S`` format. 

    Returns
    -------
    datetime.datetime
        datetime.datetime object obtained from the converted datetime string

    """

    datetime_object = datetime.strptime(time_string, '%Y%m%d-%H%M%S')

    return datetime_object

def get_runtime(start_time, end_time):
    """
    Calculate runtime between ``end_time`` and ``start_time``.

    Parameters
    ----------
    start_time : datetime.datetime
        Start time
    end_time : datetime.datetime
        End time

    Returns
    -------
    delta : str
        Runtime in ``%Y%m%d-%H%M%S`` format

    """

    delta = end_time - start_time

    return str(delta)

def archive_directory(output_filename, directory_name):
    """
    Create .zip archive from a directory called ``directory_name``.

    Parameters
    ----------
    output_filename : str
        Name of the output .zip directory
    directory_name : str
        Path to the directory to be archived
    """

    if output_filename.endswith('.zip'):
        output_filename = output_filename.split('.')[0]

    make_archive(base_name=output_filename, format='zip', base_dir=directory_name)

def delete_directory(directory_name):
    """
    Delete directory called ``directory_name``.

    Parameters
    ----------
    directory_name : str
        Path to the directory to be deleted
    """

    rmtree(directory_name)
