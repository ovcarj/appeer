import os
import sys
import time

import appeer.utils
import appeer.log

from appeer.datadir import Datadir
from appeer.db.jobs_db import JobsDB
from appeer.config import Config
from appeer.scrape.input_handling import parse_data_source, handle_input_reading
from appeer.scrape.scrape_plan import ScrapePlan
from appeer.scrape.scraper import Scraper

def scrape(scrape_label, scrape_plan, download_directory, _logger, 
        sleep_time, max_tries, retry_sleep_time,
        jobs_db):
    """
    Download publications data from a list of URLs according to the ``ScrapePlan``.

    Publications are downloaded each ``sleep_time`` seconds and written into the ``download_directory``.

    Parameters
    ----------
    scrape_label : str
        Label of the scrape job
    ScrapePlan : appeer.scrape.scrape_plan.ScrapePlan
        Instance of the ScrapePlan class containing the URL list and scraping plan
    download_directory : str
        Directory into which the HTMLs are downloaded
    _logger : logging.Logger
        logging.Logger object used to write into the logfile
    sleep_time : float
        How much time (in seconds) to sleep before sending a new request
    max_tries : int
        Maximum number of tries to get a response from an URL before giving up
    retry_sleep_time : float
        Time (in seconds) to wait before retrying an URL again
    jobs_db : appeer.db.jobs_db.JobsDB
        Instance of the JobsDB class referring to the scrape database

    """

    log_dashes = appeer.log.get_log_dashes()
    short_log_dashes = appeer.log.get_short_log_dashes()

    count_success = 0
    count_fail = 0
    failed_indices = []
    failed_urls = []

    no_of_publications = len(scrape_plan.url_list)

    jobs_db._update_scrape_job_entry(
            label=scrape_label,
            column_name='no_of_publications',
            new_value=no_of_publications)
    
    _logger.info(f'{appeer.log.boxed_message("SCRAPE JOB")}\n')
    _logger.info(f'Starting to download {no_of_publications} publications to {download_directory}\n')
    _logger.info(log_dashes)

    os.makedirs(download_directory, exist_ok=True)

    jobs_db._update_scrape_job_entry(
            label=scrape_label,
            column_name='job_status',
            new_value='R')

    for i, url in enumerate(scrape_plan.url_list):

        publisher = scrape_plan.publishers[i]
        strategy = scrape_plan.strategies[i]

        jobs_db._add_scrape(
                label=scrape_label,
                scrape_index=i,
                url=url,
                strategy=strategy
                )

        _logger.info(f'{i + 1}/{no_of_publications}: Scraping {url}')
        _logger.info(f'Publisher: {publisher}')
        _logger.info(f'Strategy: {strategy}')

        jobs_db._update_scrape_entry(
                label=scrape_label,
                scrape_index=i,
                column_name='strategy',
                new_value=strategy
                )

        scraper = Scraper(url, publisher, strategy, _logger=_logger, 
                max_tries=max_tries, retry_sleep_time=retry_sleep_time)

        jobs_db._update_scrape_entry(
                label=scrape_label,
                scrape_index=i,
                column_name='status',
                new_value='R'
                )

        scraper.run_scrape()

        if scraper.success:

            count_success += 1

            jobs_db._update_scrape_job_entry(
                label=scrape_label,
                column_name='job_successes',
                new_value=count_success)

            jobs_db._update_scrape_entry(
                    label=scrape_label,
                    scrape_index=i,
                    column_name='status',
                    new_value='X'
                    )

            if scraper._write_text:

                writing_path = f'{download_directory}/{i}_html.dat'
                appeer.utils.write_text_to_file(writing_path, 
                        scraper.response_text)
                _logger.info(f'Wrote downloaded text to {writing_path}')

                jobs_db._update_scrape_entry(
                        label=scrape_label,
                        scrape_index=i,
                        column_name='out_file',
                        new_value=f'{i}_html.dat'
                        )

        else:

            count_fail += 1
            failed_indices.append(i)
            failed_urls.append(url)

            jobs_db._update_scrape_job_entry(
                label=scrape_label,
                column_name='job_fails',
                new_value=count_fail)

            jobs_db._update_scrape_entry(
                    label=scrape_label,
                    scrape_index=i,
                    column_name='status',
                    new_value='E'
                    )

        _logger.info(f'{i + 1}/{no_of_publications}: Scraping done')

        _logger.info(log_dashes)

        time.sleep(sleep_time)

    _logger.info(f'\n{appeer.log.boxed_message("SCRAPE JOB SUMMARY")}\n')
    _logger.info('Scraping job finished!')
    _logger.info(f'Success: {count_success}/{no_of_publications}')
    _logger.info(f'Fail: {count_fail}/{no_of_publications}\n')

    if count_fail > 0:

        _logger.info(f'Failed URLs:')

        for failed_url in failed_urls:
            _logger.info(failed_url)

        _logger.info(f'Indices in the URL list of the failed scrapes:')
        _logger.info(f'{failed_indices}')

    _logger.info(log_dashes)

def main(publications, output_zip_filename=None, 
        description=None,
        sleep_time=None, max_tries=None, retry_sleep_time=None,
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
    the data will be stored in the default directories defined in the 
    appeer config file (recommended).

    Parameters
    ----------
    publications : list | str
        A list of URLs or a filepath to a JSON or plaintext file containing URLs
    output_zip_filename: str
        Name of the ZIP archive containing the downloaded data. If not given, a default name based on the timestamp is generated
    description : str
        Optional description of the scrape job
    sleep_time: float
        Time (in seconds) between sending requests. If not given, the value from the appeer config file is used
    logdir: str
        Directory in which to store the logfile. If not given, the ``appeer`` data directory is used (recommended)
    download_dir: str
        Directory into which to download the files. If not given, the default ``appeer`` data directory is used (recommended)
ir is used (recommended)
    max_tries : int
        Maximum number of tries to get a response from an URL before giving up. If not given, the value from the appeer config file is used
    retry_sleep_time : float
        Time (in seconds) to wait before trying an URL again. If not given, the value from the appeer config file is used
    cleanup: bool
        If this flag is provided, the output ZIP archive will be kept, while the directory with the downloaded data will be deleted

    """

    start_datetime = appeer.utils.get_current_datetime()
    random_number = appeer.utils.random_number()
    scrape_label = f'scrape_{start_datetime}_{random_number}'

    default_dirs = Datadir()

    cfg = Config()

    if description is None:
        description = 'No description'

    if sleep_time is None:
        sleep_time = float(cfg._config['ScrapeDefaults']['sleep_time'])

    if max_tries is None:
        max_tries = int(cfg._config['ScrapeDefaults']['max_tries'])

    if retry_sleep_time is None:
        retry_sleep_time = float(cfg._config['ScrapeDefaults']['retry_sleep_time'])

    if output_zip_filename is None:
        output_zip_filename = os.path.join(default_dirs.scrape_archives, f'{scrape_label}.zip')

    if logdir is None:
        logdir = default_dirs.scrape_logs

    if download_dir is None:
        download_dir = os.path.join(default_dirs.downloads, f'{scrape_label}')

    _logger = appeer.log.init_logger(logdir=logdir, logname=f'{scrape_label}')
    logpath = appeer.log.get_logger_fh_path(_logger)
    log_dashes = appeer.log.get_log_dashes()
    logo = appeer.log.get_logo()
    
    jobs_db = JobsDB()

    jobs_db._add_scrape_job(
            label=scrape_label,
            description=description,
            log=logpath,
            download_directory=download_dir,
            zip_file=output_zip_filename,
            date=start_datetime
            )

    start_report = appeer.log.appeer_start(start_datetime=start_datetime, logpath=logpath)
    _logger.info(start_report)
    _logger.info(description)
    _logger.info(log_dashes)

    data_source, data_source_type, plaintext_ex_message, json_ex_message = parse_data_source(publications)
    reading_passed, reading_report = handle_input_reading(publications, data_source_type, str(plaintext_ex_message), str(json_ex_message))

    _logger.info(reading_report)

    if not reading_passed:

        jobs_db._update_scrape_job_entry(
                label=scrape_label,
                column_name='job_status',
                new_value='E'
                )

        end_report = appeer.log.appeer_end(start_datetime=start_datetime)
        _logger.info(end_report)
        sys.exit()

    plan = ScrapePlan(data_source)

    _logger.info(plan._strategy_report)

    scrape(scrape_label=scrape_label,
            scrape_plan=plan,
            download_directory=download_dir, 
            _logger=_logger, sleep_time=sleep_time,
            max_tries=max_tries, retry_sleep_time=retry_sleep_time,
            jobs_db=jobs_db)

    appeer.utils.archive_directory(output_filename=output_zip_filename, directory_name=download_dir)

    if output_zip_filename.endswith('.zip'):
        zip_name_log = output_zip_filename

    else:
        zip_name_log = f'{output_zip_filename}.zip'

    _logger.info(f'Saved downloaded data to {zip_name_log} !')
    _logger.info(log_dashes)

    if cleanup:

        appeer.utils.delete_directory(download_dir)

        _logger.info(f'Deleted directory {download_dir}, as requested.')
        _logger.info(log_dashes)

    _logger.info('Job done!')

    end_report = appeer.log.appeer_end(start_datetime=start_datetime)
    _logger.info(end_report)

    jobs_db._update_scrape_job_entry(
            label=scrape_label,
            column_name='job_status',
            new_value='X'
            )

if __name__ == '__main__':

    main(sys.argv[1:])
