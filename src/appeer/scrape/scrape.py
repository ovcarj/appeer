import os
import sys
import time

import appeer.utils
import appeer.log

from appeer.datadir import Datadir
from appeer.scrape.input_handling import parse_data_source, handle_input_reading
from appeer.scrape.scrape_plan import ScrapePlan
from appeer.scrape.scraper import Scraper

def scrape(scrape_plan, download_directory, _logger, sleep_time=0.5):
    """
    Download publications data from a list of URLs according to the ``ScrapePlan``.

    Publications are downloaded each ``sleep_time`` seconds and written into the ``download_directory``.

    Parameters
    ----------
    ScrapePlan : appeer.scrape.ScrapePlan.ScrapePlan
        Instance of the ScrapePlan class containing the URL list and scraping plan
    download_directory : str
        Directory into which the HTMLs are downloaded
    _logger : logging.Logger
        logging.Logger object used to write into the logfile
    sleep_time : float
        How much time (in seconds) to sleep before sending a new request

    """

    log_dashes = appeer.log.get_log_dashes()
    short_log_dashes = appeer.log.get_short_log_dashes()

    count_success = 0
    count_fail = 0
    failed_indices = []
    failed_urls = []

    no_of_publications = len(scrape_plan.url_list)
    
    _logger.info(log_dashes)
    _logger.info(f'Starting download of {no_of_publications} publications to {download_directory}')
    _logger.info(log_dashes)

    os.makedirs(download_directory, exist_ok=True)

    for i, url in enumerate(scrape_plan.url_list):

        publisher = scrape_plan.publishers[i]
        strategy = scrape_plan.strategies[i]

        _logger.info(f'{i + 1}/{no_of_publications}: Scraping {url} ...')
        _logger.info(f'Publisher: {publisher}')
        _logger.info(f'Strategy: {strategy}')

        scraper = Scraper(url, publisher, strategy, _logger=_logger)

        _logger.info(f'Starting scrape...')

        scraper.run_scrape()

        if scraper.success:

            count_success += 1

            if scraper._write_text:

                writing_path = f'{download_directory}/{i}_html.dat'
                appeer.utils.write_text_to_file(writing_path, 
                        scraper.response_text)
                _logger.info(f'Wrote downloaded text to {writing_path}.')

        else:

            count_fail += 1
            failed_indices.append(i)
            failed_urls.append(url)

        _logger.info(f'{i + 1}/{no_of_publications}: Scraping done.')

        if i < (no_of_publications - 1):
            _logger.info(short_log_dashes)

        time.sleep(sleep_time)

    _logger.info(log_dashes)
    _logger.info('Scraping finished!')
    _logger.info(f'Success: {count_success}/{no_of_publications}')
    _logger.info(f'Fail: {count_fail}/{no_of_publications}')

    if count_fail > 0:

        _logger.info(f'Failed URLs:')

        for failed_url in failed_urls:
            _logger.info(failed_url)

        _logger.info(f'Indices in the URL list of the failed scrapes:')
        _logger.info(f'{failed_indices}')

    _logger.info(log_dashes)

def main(publications, output_zip_filename=None, sleep_time=0.5, 
        logdir=None, download_dir=None,
        cleanup=False):
    """ 
    Download publications data for later parsing.

    Example usage: appeer scrape -c PoP.json

    ``publications`` can be a list of URLs or a string (path to a file).
    In the case a filepath is provided, the file can be either a JSON file 
    (e.g. ``PoP.json`` file containing ``['article_url']`` keys)
    or a plaintext file with each URL in a new line.

    If ``output_zip_filename``, ``logdir`` and ``download_dir`` are not given,
    the data will be stored in the default directories determined by ``platformdirs`` 
    (this is recommended).

    Parameters
    ----------
    publications : list | str
        A list of URLs or a filepath to a JSON or plaintext file containing URLs
    output_zip_filename: str
        Name of the ZIP archive containing the downloaded data. If not given, a default name based on the timestamp is generated
    sleep_time: float
        Time (in seconds) between sending requests
    logdir: str
        Directory in which to store the logfile. If not given, default ``appeer`` Datadir is used (recommended)
    download_dir: str
        Directory into which to download the files. If not given, default ``appeer`` Datadir is used (recommended)
ir is used (recommended)
    cleanup: bool
        If this flag is provided, the output ZIP archive will be kept, while the directory with the downloaded data will be deleted

    """

    start_datetime = appeer.utils.get_current_datetime()
    random_number = appeer.utils.random_number()
    scrape_label = f'appeer-scrape_{start_datetime}_{random_number}'

    default_dirs = Datadir()

    if output_zip_filename is None:
        output_zip_filename = f'{default_dirs.scrape_archives}/{scrape_label}.zip'

    if logdir is None:
        logdir = default_dirs.scrape_logs

    if download_dir is None:
        download_dir = f'{default_dirs.downloads}/{scrape_label}'

    _logger = appeer.log.init_logger(logdir=logdir, logname=f'{scrape_label}')
    logpath = appeer.log.get_logger_fh_path(_logger)
    log_dashes = appeer.log.get_log_dashes()
    logo = appeer.log.get_logo()

    start_report = appeer.log.appeer_start(start_datetime=start_datetime, logpath=logpath)
    _logger.info(start_report)

    data_source, data_source_type, plaintext_ex_message, json_ex_message = parse_data_source(publications)
    reading_passed, reading_report = handle_input_reading(publications, data_source_type, str(plaintext_ex_message), str(json_ex_message))

    _logger.info(reading_report)

    if not reading_passed:
        end_report = appeer.log.appeer_end(start_datetime=start_datetime)
        _logger.info(end_report)
        sys.exit()

    plan = ScrapePlan(data_source)

    _logger.info(plan._strategy_report)

    scrape(scrape_plan=plan, 
            download_directory=download_dir, 
            _logger=_logger, sleep_time=sleep_time)

    appeer.utils.archive_directory(output_filename=output_zip_filename, directory_name=download_dir)

    if output_zip_filename.endswith('.zip'):
        zip_name_log = output_zip_filename

    else:
        zip_name_log = f'{output_zip_filename}.zip'

    _logger.info(f'Saved downloaded data to {zip_name_log} !')
    _logger.info(log_dashes)

    if cleanup:

        appeer.utils.delete_directory(download_directory)

        _logger.info(f'Deleted directory {download_dir}, as requested.')
        _logger.info(log_dashes)

    _logger.info('Job done!')

    end_report = appeer.log.appeer_end(start_datetime=start_datetime)
    _logger.info(end_report)

if __name__ == '__main__':

    main(sys.argv[1:])
