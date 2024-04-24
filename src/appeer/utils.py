import sys
import time
import json
import logging

from shutil import make_archive, rmtree
from datetime import datetime

def _init_logger(start_time, name='appeer'):

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO) 
    stream_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stream_handler)
    file_handler = logging.FileHandler(f'{name}_{start_time}.log')
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    return logger

def get_log_dashes():

    return '------------------------------------'

def get_logo():

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

    with open(json_filename, encoding='utf-8-sig') as f:
        data = json.load(f)

    return data

def write_text_to_file(path_to_file, text_data):

	with open(path_to_file, 'w+') as f:
            f.write(text_data)

def get_current_datetime():

    timestr = time.strftime("%Y%m%d-%H%M%S")

    return timestr

def convert_time_string(time_string):

    datetime_object = datetime.strptime(time_string, '%Y%m%d-%H%M%S')

    return datetime_object

def get_runtime(start_time, end_time):

    delta = end_time - start_time

    return str(delta)

def archive_directory(output_filename, directory_name):

    if output_filename.endswith('.zip'):
        output_filename = output_filename.split('.')[0]

    make_archive(base_name=output_filename, format='zip', base_dir=directory_name)

def delete_directory(directory_name):

    rmtree(directory_name)
