import os
import pathlib
import logging
import glob

import appeer.utils
import appeer.log

def test_init_logger(tmp_path):
    """
    Test if the logger is initialized correctly.
    """

    tmp_dir = tmp_path / 'test_log'
    tmp_dir.mkdir()

    start_directory = os.getcwd()
    os.chdir(tmp_dir)

    logname = 'log_test'
    _logger = appeer.log.init_logger(logname=logname)

    os.chdir(start_directory)

    assert type(_logger) == logging.Logger
    
    logtext = 'Log test'
    _logger.info(logtext)

    logfile = glob.glob(f'{str( tmp_dir.resolve() )}/{logname}*')[0]

    assert pathlib.Path(logfile).is_file()

    with open(logfile, 'r') as f:
        text = f.read()

        assert text == logtext + '\n'

def test_get_log_dashes():
    """
    Test if at least one dash for logging is created.
    """

    dashes = appeer.log.get_log_dashes()

    assert type(dashes) == str
    assert len(dashes) > 0
    assert '-' in dashes

def test_get_logo():
    """
    Test if the logo is a string.
    """

    logo = appeer.log.get_logo()

    assert type(logo) == str
    assert len(logo) > 0
