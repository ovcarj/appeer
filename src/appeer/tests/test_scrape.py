import pytest
import os
import argparse
import requests
import pathlib
import glob

import appeer.scrape
import appeer.utils
import appeer.log

def test_parse_input_arguments():
    """
    Test if input arguments of ``appeer-scrape`` are parsed correctly.
    """

    arguments = ['-i', 'in.json', '-o', 'out.zip', '-t', '5.0',
            '-l', 'logdir', '-d', 'download_dir' , '-c', '-q']

    args = appeer.scrape.parse_input_arguments(arguments)

    assert type(args) == argparse.Namespace

    assert args.input == arguments[1]
    assert args.output == arguments[3]
    assert args.sleep_time == float(arguments[5])
    assert args.logdir == arguments[7]
    assert args.download_dir == arguments[9]
    assert args.quiet == True
    assert args.cleanup == True 

    args = appeer.scrape.parse_input_arguments(arguments[:-6])

    assert args.logdir is None
    assert args.download_dir is None
    assert args.quiet == False
    assert args.cleanup == False

def test_initialize_headers():
    """
    Test if the created header is of correct type and nonempty.
    """

    headers = appeer.scrape.initialize_headers()

    assert type(headers) == requests.structures.CaseInsensitiveDict

    for key in headers.keys():
        assert headers[key]

@pytest.mark.slow
def test_scrape(tmp_path, sample_json_path):
    """
    Test if the files downloaded using the sample JSON are nonempty
    and if the logfile is nonempty.
    """

    tmp_dir = tmp_path / 'test_scrape'
    tmp_dir.mkdir()
    tmp_dir_str = str(tmp_dir)

    _logger = appeer.log.init_logger(logdir=tmp_dir_str, logname='scrape-test')

    sample_json = appeer.utils.load_json(sample_json_path)
    sleep_time = 0.5

    appeer.scrape.scrape(publications_list=sample_json, 
            download_directory=f'{tmp_dir_str}/htmls', 
            _logger=_logger,
            sleep_time=sleep_time)

    no_of_publications = len(sample_json)

    downloaded_files = glob.glob(f'{tmp_dir_str}/htmls/*')

    assert len(downloaded_files) == no_of_publications

    for filepath in downloaded_files:
        assert os.path.getsize(filepath) > 0
    
    logfile = glob.glob(f'{tmp_dir_str}/*scrape-test*')[0]
    assert os.path.getsize(logfile) > 0

@pytest.mark.slow
def test_main(tmp_path, sample_json_path):
    """
    Test if zipping and cleanup works, the download is tested in ``test_scrape``.
    """

    tmp_dir = tmp_path / 'test_scrape-main'
    tmp_dir.mkdir()
    tmp_dir_str = str(tmp_dir)

    sleep_time = 0.5

    download_directory = f'{tmp_dir_str}/download'
    output_zip_path = f'{tmp_dir_str}/out.zip'

    arguments = ['-i', f'{sample_json_path}', '-o', f'{output_zip_path}', 
            '-l', f'{tmp_dir_str}', '-d', f'{download_directory}',
            '-t', f'{sleep_time}', '-c']

    appeer.scrape.main(arguments)

    assert os.path.getsize(f'{tmp_dir_str}/out.zip') > 0

    logfile = glob.glob(f'{tmp_dir_str}/appeer-scrape*')[0]
    assert os.path.getsize(logfile) > 0

    assert not os.path.exists(f'{download_directory}')
