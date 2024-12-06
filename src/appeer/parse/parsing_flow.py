import os

from appeer.general import log
from appeer.general import utils

from appeer.general.datadir import Datadir
from appeer.db.jobs_db import JobsDB

def initialize_parse_job(mode,
        description=None,
        logdir=None, parse_directory=None):
    """
    Initializes a parse job of a given ``mode``.

    Available modes: auto, scrape_job, files

    auto:       finds all unparsed scrapes in the jobs.db database and parses them
    everything: parse all scrapes, regardless of whether they were already parsed
    scrape_job: parse a scrape job with a given ``label``
    files:      parse a list of files

    Parameters
    ----------
    mode : str
        Parsing mode. Must be one of ['automatic', 'scrape_job', 'files']
    description : str
        Optional description of the parse job
    logdir : str
        Directory in which to store the log. If not given, the default appeer 
        data directory is used.
    parse_directory : str
        Path to the directory where (temporary) files for parsing are created

    Returns
    -------
    parse_label : str
        Label of the parse job
    _logger : logging.Logger
        logging.Logger object
    start_datetime : str
        Date and time when the parse job started

    """

    start_datetime = utils.get_current_datetime()
    random_number = utils.random_number()
    parse_label = f'parse_{start_datetime}_{random_number}'

    default_dirs = Datadir()

    if description is None:
        description = 'No description'

    if logdir is None:
        logdir = default_dirs.parse_logs

    if parse_directory is None:
        parse_directory = os.path.join(default_dirs.parse, parse_label)

    _logger = log.init_logger(logdir=logdir, logname=f'{parse_label}')
    logpath = log.get_logger_fh_path(_logger)
    log_dashes = log.get_log_dashes()

    jobs_db = JobsDB()

    jobs_db.parse_jobs.add_entry(
            label=parse_label,
            description=description,
            log_path=logpath,
            mode=mode,
            parse_directory=parse_directory,
            date=start_datetime
            )

    start_report = log.appeer_start(start_datetime=start_datetime, logpath=logpath)
    _logger.info(start_report)
    _logger.info(description)
    _logger.info(log_dashes)

    _logger.info(f'Parse job {parse_label} initialized. (mode {mode})')
    _logger.info(log_dashes)

    return parse_label, _logger, start_datetime
