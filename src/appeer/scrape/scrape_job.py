"""Scrape publications for later parsing"""

import os

from appeer.jobs.job import Job

from appeer.general.datadir import Datadir

class ScrapeJob(Job, job_type='scrape_job'):
    """
    Scrape publications for later parsing

    """

    def __init__(self, label=None, job_mode='read'):
        """
        Connects to the job database and sets the job label

        Parameters
        ----------
        label : str
            Unique job label
        job_mode : str
            Must be 'read' or 'write'

        """

        super().__init__(label=label, job_mode=job_mode)

    def _initialize_job(self,
            label=None,
            description=None,
            log_dir=None,
            download_directory=None,
            zip_file=None):
        """
        Initializes a new scrape job

        Parameters
        ----------
        label : str
            Unique label of the scrape job
        description : str
            Optional job description
        log_dir : str
            Directory in which to store the log file
        download_directory : str
            Directory in which to download the data
        zip_file : str
            Path to the output ZIP file

        """

        self._qualify_job_label(label=label)
        self._job_mode = 'write'

        datadir = Datadir()

        if not download_directory:
            download_directory = os.path.join(datadir.downloads, self.label)

        if not zip_file:
            zip_file = os.path.join(datadir.scrape_archives,
                    f'{self.label}.zip')

        else:
            if not zip_file.endswith('.zip'):
                zip_file += '.zip'

        self._initialize_job_common(description=description,
                log_dir=log_dir,
                download_directory=download_directory,
                zip_file=zip_file,)
