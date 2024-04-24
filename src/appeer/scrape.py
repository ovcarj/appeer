import os
import argparse
import time
import requests

import appeer.utils

def parse_input_arguments():
    """
    Parse input arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input', type=str,
            help='Input JSON filename', required=True)
    parser.add_argument('-o', '--output', type=str,
            help='Name of the output ZIP archive', required=True)
    parser.add_argument('-t', '--sleep_time', type=float,
            help='Time (in seconds) between sending requests', required=False, default=1.0)
    parser.add_argument('-c', '--cleanup', action='store_true',
            help='If this flag is provided, the output ZIP archive will be kept, while the directory with the downloaded data will be deleted', required=False)
    parser.add_argument('-q', '--quiet', action='store_true',
            help='If this flag is provided, no logging will be done (not recommended)', required=False)

    args = parser.parse_args()

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

def scrape(publications_list, download_directory, quiet=False, sleep_time=0.5):
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
    quiet : bool
        If true, no logging is done (not recommended)
    sleep_time : float
        How much time (in seconds) to sleep before sending a new request

    """

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

def main():
    """ 
    Load the input ``PoP.json`` file into a list of dictionaries and download HTMLs found in the ``article_url`` dictionary keys.
    """

    start_datetime = appeer.utils.get_current_datetime()

    args = parse_input_arguments()

    json_filename = args.input
    zip_filename = args.output
    sleep_time = args.sleep_time
    quiet = args.quiet
    cleanup = args.cleanup

    if not quiet:

        global _logger 
        global log_dashes
        _logger = appeer.utils._init_logger(start_time=start_datetime, logname='appeer-scrape')
        log_dashes = appeer.utils.get_log_dashes()
        logo = appeer.utils.get_logo()

        _logger.info(logo)
        _logger.info(log_dashes)
        _logger.info(f'appeer-scrape started on {start_datetime}')
        _logger.info(log_dashes)
 
    if not quiet:
        _logger.info(f'Reading data from {json_filename}...')

    publications_list = appeer.utils.load_json(json_filename)

    if not quiet:
        _logger.info(f'Successfully read data from {json_filename}!')
        _logger.info(log_dashes)

    download_directory = f'appeer-scrape_{start_datetime}'

    scrape(publications_list=publications_list, 
            download_directory=download_directory, 
            quiet=quiet, sleep_time=sleep_time)

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

    main()
