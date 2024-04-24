import os
import argparse
import time
import requests

import appeer.utils

def initialize_headers():

    headers = requests.utils.default_headers()
    headers.update({'User-Agent': 'My User Agent 1.0'})

    return headers

def scrape_htmls(publications_list, download_directory, quiet=False, sleep_time=0.5):

    no_of_publications = len(publications_list)
    requests_headers = initialize_headers()
    
    if not quiet:
        _logger.info(f'Downloading {no_of_publications} HTMLs to {download_directory}')
        _logger.info(log_dashes)

    os.makedirs(download_directory, exist_ok=True)

    for i, publication in enumerate(publications_list):

        url = publication['article_url']

        if not quiet:
            _logger.info(f'{i + 1}/{no_of_publications}: Scraping {url} ...')

        response = requests.get(url, headers=requests_headers)

        appeer.utils.write_text_to_file(f'{download_directory}/{i}_html.dat', response.text)

        time.sleep(sleep_time)

    if not quiet:
        _logger.info(log_dashes)
        _logger.info('All files downloaded!')
        _logger.info(log_dashes)

def main():

    global _logger 
    global log_dashes
    _logger = appeer.utils._init_logger()
    log_dashes = appeer.utils.get_log_dashes()
    logo = appeer.utils.get_logo()

    current_datetime = appeer.utils.get_current_datetime()
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input', type=str, 
            help='Input JSON filename', required=True)
    parser.add_argument('-o', '--output', type=str, 
            help='Name of the output ZIP archive', required=True)
    parser.add_argument('-t', '--sleep_time', type=float, 
            help='Time (in seconds) between sending requests', required=False, default=1.0)
    parser.add_argument('-c', '--cleanup', action=argparse.BooleanOptionalAction, 
            help='If this flag is provided, the output GZIP archive will be kept, while the downloaded data will be deleted', required=False)
    parser.add_argument('-q', '--quiet', action=argparse.BooleanOptionalAction, 
            help='Don\'t print progress during download', required=False)

    args = parser.parse_args()

    json_filename = args.input
    zip_filename = args.output
    sleep_time = args.sleep_time
    quiet = args.quiet
    cleanup = args.cleanup

    if not quiet:
        _logger.info(logo)
        _logger.info(log_dashes)
        _logger.info(f'appeer started on {current_datetime}')
        _logger.info(log_dashes)
        _logger.info(f'Reading data from {json_filename}...')

    publications_list = appeer.utils.load_json(json_filename)

    if not quiet:
        _logger.info(f'Successfully read data from {json_filename}!')
        _logger.info(log_dashes)

    download_directory = f'appeer_scrape_{current_datetime}'

    scrape_htmls(publications_list=publications_list, 
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

if __name__ == '__main__':

    main()
