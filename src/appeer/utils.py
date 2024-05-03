import os
import glob
import re
import sys
import time
import json

import click

import pathlib
from shutil import make_archive, rmtree
from datetime import datetime
from random import randint

def load_json(json_filename):
    """
    Load a JSON file to a list of dictionaries.

    Parameters
    ----------
    json_filename : str
        Input JSON filename

    Returns
    -------
    list
        List of dictionaries generated from the JSON file

    """

    with open(json_filename, encoding='utf-8-sig') as f:
        data = json.load(f)

    return data

def json2list(json_filename):
    """
    Convert a JSON file to a Python list containing only article URLs.
    If an article URL cannot be found for a given entry,
    'no_url' will be written into the output list.

    Parameters
    ----------
    json_filename : str
        Input JSON filename

    Returns
    -------
    url_list : list
        List of URLs parsed from the JSON file

    """

    loaded_json = load_json(json_filename)
    url_list = []

    for i, publication in enumerate(loaded_json):

        try:
            url = publication['article_url']

        except KeyError:
            url = f'no_url'

        url_list.append(url)

    return url_list

def json2txt(json_filename, text_filename):
    """
    Convert a JSON file to a text file containing only article URLs.
    The URLs are extracted using the ``'article_url'`` keys from the JSON entries.

    Parameters
    ----------
    json_filename : str
        Input JSON filename
    text_filename : str
        Output textfile filename
    """

    url_list = json2list(json_filename)

    with open(text_filename, 'w+') as f:
        for url in url_list:
            f.write(f'{url}\n')

def txt2list(text_filename):
    """
    Convert a text file to a Python list containing only article URLs.
    Each URL should be written in a separate line.

    The URLs can be written either as a full URL or a DOI.
    E.g., the following entries are equally valid:

    https://pubs.rsc.org/en/content/articlelanding/2023/ob/d3ob00424d
    10.1039/D3OB00424D

    Valid entries start with 'https://' or need to be in DOI format: 10.prefix/suffix.
    If the entry format is invalid, 'no_url' is written into the output list.

    If any line contains more than one entry, an exception is raised.

    Parameters
    ----------
    text_filename : str
        Input text filename

    Returns
    -------
    url_list : list
        List of URLs parsed from the text file

    """

    url_list = []

    with open(text_filename, 'r') as f:

        lines = f.readlines()

    for i, line in enumerate(lines):

        entry = line.split('\n')[0]

        if len(entry.split(' ')) > 1:
            raise ValueError(f'More than one URL entry on line #{i} in file {text_filename}')

        else:

            if entry.startswith('https://'):
                url_list.append(entry)

            elif is_doi_format(entry):
                url_list.append(f'https://doi.org/{entry}')

            else:
                url_list.append('no_url')

    return url_list

def is_doi_format(entry):
    """
    Check if a string is in DOI format (10.prefix/suffix).

    Parameters
    ----------
    entry : str
        String to be tested if it is in DOI format

    Returns
    ----------

    is_doi_format : bool
        True if ``entry`` is in DOI format, False if it is not
    """

    starts_w_10 = entry.startswith('10')
    has_prefix_suffix = (len(re.split('\.|/', entry)) == 3)

    if (starts_w_10 and has_prefix_suffix):
        is_doi_format = True

    else:
        is_doi_format = False

    return is_doi_format

def get_current_datetime():
    """
    Get current datetime in the ``%Y%m%d-%H%M%S`` format.

    Returns
    -------
    timestr : str
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
        Runtime in ``%H:%M:%S`` format

    """

    delta = end_time - start_time

    return str(delta)

def random_number():
    """
    Generate a random number as a string (used for naming download directories and logs).

    Returns
    -------
    random_no : str
        Random number between 0 and 100

    """

    random_no = str(randint(0, 100))

    return random_no


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
        output_filename = output_filename[:-4]

    make_archive(base_name=output_filename, format='zip', base_dir=directory_name)

def delete_directory(directory_name):
    """
    Delete directory called ``directory_name``.

    Parameters
    ----------
    directory_name : str
        Path to the directory to be deleted
    """

    if directory_exists(directory_name):

        rmtree(directory_name)
    
        if not directory_exists(directory_name):
            click.echo(f'Deleted {directory_name}')

        else:
            click.echo(f'Could not delete {directory_name}')

    else:
        click.echo(f'Directory {directory_name} does not exist')

def directory_exists(directory_path):
    """
    Check if directory ``directory_path`` exists.

    Parameters
    ----------
    directory_path : str
        Path to the directory whose existence is checked
 
    Returns
    ----------
    exists : bool
        True if directory exists, False if it does not.

    """

    exists = os.path.isdir(directory_path)

    return exists

def file_exists(file_path):
    """
    Check if directory ``filepath`` exists.

    Parameters
    ----------
    file_path : str
        Path to the file whose existence is checked
 
    Returns
    ----------
    exists : bool
        True if file exists, False if it does not.

    """

    exists = os.path.isfile(file_path)

    return exists

def is_directory_empty(directory_path):
    """
    Check if directory ``directory_path`` is not empty.

    Parameters
    ----------
    directory_path : str
        Path to the directory that is checked for existence of any content
 
    Returns
    ----------
    empty : bool
        True if directory is empty, False if it is not.

    """

    exists = directory_exists(directory_path)

    if exists:
        empty = not os.listdir(directory_path)

    else:
        click.echo(f'Directory {directory_path} does not exist.')
        empty = True

    return empty

def get_files_in_dir(directory_path):
    """
    Get a list of files in a directory (excluding subdirectories).

    Parameters
    ----------
    directory_path : str
        Path to the directory in which to search for files
    Returns
    ----------
    files : list
        List of filepaths in the searched directory

    """
    
    files = glob.glob(f'{directory_path}/*')
    files = [f for f in files if os.path.isfile(f)]

    return files

def delete_directory_files(directory_path):
    """
    Deletes files in a directory (does not delete subdirectories and their file content).

    Parameters
    ----------
    directory_path : str
        Path to the directory whose files are to be deleted

    """

    files = get_files_in_dir(directory_path)

    if files:
        for f in files:
            try:
                os.remove(f)
            except:
                click.echo(f'Could not delete file {f}.')
    else:
        click.echo('Nothing to delete.')
        return

    files_after = get_files_in_dir(directory_path)

    if not files_after:
        click.echo(f'Files in {directory_path} deleted.')

    else:
        click.echo(f'Could not delete all files in {directory_path}')

def delete_directory_content(directory_path):
    """
    Deletes everything in a directory.

    Parameters
    ----------
    directory_path : str
        Path to the directory whose contents are to be deleted

    """

    contents = glob.glob(f'{directory_path}/*')
    
    if len(contents) == 0:
        click.echo('Nothing to delete.')
        return

    for root, dirs, files in os.walk(directory_path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            rmtree(os.path.join(root, d))

    contents_after = glob.glob(f'{directory_path}/*')

    if not contents_after:
        click.echo(f'Contents of {directory_path} deleted.')

    else:
        click.echo(f'Could not delete all files in {directory_path}')
