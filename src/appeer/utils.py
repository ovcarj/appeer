import sys
import time
import json
import logging

from shutil import make_archive, rmtree

def _init_logger(name='appeer'):

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO) 
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)

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

def archive_directory(output_filename, directory_name):

    if output_filename.endswith('.zip'):
        output_filename = output_filename.split('.')[0]

    make_archive(base_name=output_filename, format='zip', base_dir=directory_name)

def delete_directory(directory_name):

    rmtree(directory_name)
