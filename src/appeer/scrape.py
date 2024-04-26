import os
import sys
import argparse
import time
import requests

import appeer.utils
import appeer.log

def parse_input_arguments(arguments):
    """
    Parse input arguments of ``appeer-scrape``. For possible arguments, see ``appeer-scrape -h``.

    Parameters
    ----------
    input_arguments : list
        Input arguments. Normally inputted through the CLI using ``appeer-scrape <input_arguments>``

    Returns
    -------
    argparse.ArgumentParser
        Arguments parsed into the ArgumentParser object
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input', type=str,
            help='Input JSON filename', required=True)
    parser.add_argument('-o', '--output', type=str,
            help='Name of the output ZIP archive', required=True)
    parser.add_argument('-t', '--sleep_time', type=float,
            help='Time (in seconds) between sending requests', required=False, default=1.0)
    parser.add_argument('-l', '--logdir', type=str,
            help='Directory in which to store the logfile. Defaults to CWD', required=False, default=None)
    parser.add_argument('-d', '--download_dir', type=str,
            help='Directory into which to download the files. Defaults to appeer-scrape_{current__datetime}.', required=False, default=None)
    parser.add_argument('-c', '--cleanup', action='store_true',
            help='If this flag is provided, the output ZIP archive will be kept, while the directory with the downloaded data will be deleted', required=False)
    parser.add_argument('-q', '--quiet', action='store_true',
            help='If this flag is provided, no logging will be done (not recommended)', required=False)

    args = parser.parse_args(arguments)

    return args

def initialize_headers():
    """ 
    Create a default header using ``requests.utils.default_headers()``.

    Returns
    -------
    requests.structures.CaseInsensitiveDict
        Default requests header

    """

    headers = requests.utils.default_headers()
    headers.update({'User-Agent': 'My User Agent 1.0'})

    return headers

def scrape(publications_list, download_directory, _logger, quiet=False, sleep_time=0.5):
    """
    Download HTMLs from a list of URLs found in the loaded ``publications_list`` JSON file.

    The ``publications_list`` is obtained by ``appeer.utils.load_json('JSON_filename')``. 
    HTMLs are downloaded each ``sleep_time`` seconds and written into the ``download_directory``.

    Parameters
    ----------
    publications_list : list
        List obtained by loading the PoP.json file
    download_directory : str
        Directory into which the HTMLs are downloaded
    logger : logging.Logger
        logging.Logger object used to write into the logfile
    quiet : bool
        If true, no logging is done (not recommended)
    sleep_time : float
        How much time (in seconds) to sleep before sending a new request

    """

    log_dashes = appeer.log.get_log_dashes()

    no_of_publications = len(publications_list)
    requests_headers = initialize_headers()
    
    if not quiet:
        _logger.info(f'Downloading {no_of_publications} HTMLs to {download_directory}')
        _logger.info(log_dashes)

    os.makedirs(download_directory, exist_ok=True)

    for i, publication in enumerate(publications_list):

        # TODO: check validity of the publication dictionary
        url = publication['article_url']

        if not quiet:
            # TODO: make scraping logging more detailed
            _logger.info(f'{i + 1}/{no_of_publications}: Scraping {url} ...')

        response = requests.get(url, headers=requests_headers)
        # TODO: check validity of response

        appeer.utils.write_text_to_file(f'{download_directory}/{i}_html.dat', response.text)

        time.sleep(sleep_time)

    if not quiet:
        _logger.info(log_dashes)
        _logger.info('All files downloaded!')
        _logger.info(log_dashes)

def main(input_arguments=None):
    """ 
    Load the input ``PoP.json`` file into a list of dictionaries and download HTMLs found in the ``article_url`` dictionary keys.

    CLI usage example: ``appeer-scrape -i Pop.json -o out.zip -c``

    Parameters
    ----------
    input_arguments : list
        Input arguments, as defined in ``parse_input_arguments`` (normally inputted through the CLI using ``appeer-scrape <input_arguments>``)

    """

    start_datetime = appeer.utils.get_current_datetime()

    if input_arguments is None:
        input_arguments=sys.argv[1:]

    args = parse_input_arguments(arguments=input_arguments)

    json_filename = args.input
    zip_filename = args.output
    sleep_time = args.sleep_time
    logdir = args.logdir
    download_directory = args.download_dir
    quiet = args.quiet
    cleanup = args.cleanup

    if logdir is None:
        logdir = os.getcwd()

    logpath = os.path.abspath(logdir + f'/appeer-scrape_{start_datetime}.log')

    if not quiet:

        _logger = appeer.log.init_logger(start_time=start_datetime, logdir=logdir, logname='appeer-scrape')
        log_dashes = appeer.log.get_log_dashes()
        logo = appeer.log.get_logo()

        _logger.info(logo)
        _logger.info(log_dashes)
        _logger.info(f'appeer-scrape started on {start_datetime}')
        _logger.info(log_dashes)
        _logger.info(f'Logfile: {logpath}')
        _logger.info(log_dashes)
 
    if not quiet:
        _logger.info(f'Reading data from {json_filename}...')

    publications_list = appeer.utils.load_json(json_filename)

    if not quiet:
        _logger.info(f'Successfully read data from {json_filename}!')
        _logger.info(log_dashes)

    if download_directory is None:
        download_directory = f'appeer-scrape_{start_datetime}'

    if not quiet:
        _logger.info(f'Data will be downloaded to {os.path.abspath(download_directory)}')
        _logger.info(log_dashes)

    scrape(publications_list=publications_list, 
            download_directory=download_directory, 
            _logger=_logger, quiet=quiet, sleep_time=sleep_time)

    appeer.utils.archive_directory(output_filename=zip_filename, directory_name=download_directory)

    if not quiet:

        if zip_filename.endswith('.zip'):
            zip_name_log = zip_filename

        else:
            zip_name_log = f'{zip_filename}.zip'

        _logger.info(f'Saved downloaded data to {zip_name_log}!')
        _logger.info(log_dashes)

    if cleanup:

        appeer.utils.delete_directory(download_directory)

        if not quiet:
            _logger.info(f'Deleted directory {download_directory}, as requested.')
            _logger.info(log_dashes)

    if not quiet:

        _logger.info('Job done!')
        _logger.info(log_dashes)

        end_datetime = appeer.utils.get_current_datetime()

        runtime = appeer.utils.get_runtime(appeer.utils.convert_time_string(start_datetime), 
                appeer.utils.convert_time_string(end_datetime))

        _logger.info(f'appeer-scrape finished on {end_datetime}')
        _logger.info(f'Total runtime: {runtime}')

if __name__ == '__main__':

    main(sys.argv[1:])
