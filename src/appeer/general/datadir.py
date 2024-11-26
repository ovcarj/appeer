"""
Handles creation, deletion and checking of the ``appeer`` data directory,
which is defined in the ``appeer`` configuration file
"""

import os
import click

from appeer.general import log
from appeer.general import utils

from appeer.general.config import Config

class Datadir:
    """
    Handles creation, deletion and checking of the
    ``appeer`` data directory defined in the ``appeer``
    configuration file.

    The default location is given by 
    ``platformdirs.user_data_dir(appname='appeer')``.

    """

    def __init__(self):
        """
        Defines paths to directories in the base directory.

        """

        config = Config()

        self.base = config._base_directory

        self.downloads = os.path.join(self.base, 'downloads')

        self.scrape_archives = os.path.join(self.base, 'scrape')
        self.scrape_logs = os.path.join(self.base, 'scrape_logs')

        self.parse = os.path.join(self.base, 'parse')
        self.parse_logs = os.path.join(self.base, 'parse_logs')

        self.db = os.path.join(self.base, 'db')

        self.check_existence()

        self._dashes = log.get_log_dashes()

    def check_existence(self):
        """
        Checks the existence of the ``self.base`` directory; 
        the ``self._base_exists`` attribute is updated accordingly.
        """

        self._base_exists = utils.directory_exists(self.base)

    def create_directories(self):
        """
        Creates ``appeer`` data directories. If ``self.base`` already exists, 
        the user is prompted if they want to overwrite the directory.

        """

        if self._base_exists:

            overwrite = log.ask_yes_no(f'WARNING: appeer data base directory exists at {self.base}\nDo you want to overwrite it? All data will be deleted. [Y/n]\n')

            if overwrite == 'Y':

                self.clean_all_directories()

                click.echo(f'{self.base} deleted, as requested. Continuing...')
                click.echo(self._dashes)

            elif overwrite == 'n':

                click.echo('Stopping appeer data directory overwriting.')
                click.echo(self._dashes)

                return

        os.makedirs(self.base)
        click.echo(f'appeer base directory created at {self.base}')

        self.check_existence()

        os.makedirs(self.downloads)
        click.echo(f'appeer downloads directory created at {self.downloads}')

        os.makedirs(self.scrape_archives)
        click.echo(f'appeer scrape_archives directory created at {self.scrape_archives}')
        os.makedirs(self.scrape_logs)
        click.echo(f'appeer scrape_logs directory created at {self.scrape_logs}')

        os.makedirs(self.parse)
        click.echo(f'appeer parse directory created at {self.parse}')
        os.makedirs(self.parse_logs)
        click.echo(f'appeer parse_logs directory created at {self.parse_logs}')

        os.makedirs(self.db)
        click.echo(f'appeer db directory created at {self.db}')

    def clean_all_directories(self):
        """
        Deletes all ``appeer`` data directories.

        """

        self.check_existence()

        if not self._base_exists:
            click.echo('Nothing to clean.')

        else:
            utils.delete_directory(self.base)

        self.check_existence()

    def clean_downloads(self):
        """
        Deletes the contents of ``self.downloads``

        """

        utils.delete_directory_content(self.downloads)

    def clean_scrape_archives(self):
        """
        Deletes the files in ``self.scrape_archives``

        """

        utils.delete_directory_files(self.scrape_archives)

    def clean_scrape_logs(self):
        """
        Deletes the files in ``self.scrape_logs``

        """

        utils.delete_directory_files(self.scrape_logs)

    def clean_parse(self):
        """
        Deletes the files in ``self.parse``

        """

        utils.delete_directory_files(self.parse)

    def clean_parse_logs(self):
        """
        Deletes the files in ``self.parse_logs``

        """

        utils.delete_directory_files(self.parse_logs)

    def clean_db(self):
        """
        Deletes the files in ``self.db``

        """

        utils.delete_directory_files(self.db)

    def clean_scrape_job_data(self, scrape_label, download_directory, zip_file, log):
        """
        Deletes all data associated with a scrape job.

        Parameters
        ----------
        scrape_label : str
            Label of the scrape job whose data is being deleted
        download_directory : str
            Path to the directory where the data was downloaded
        zip_file : str
            Path to the output ZIP file
        log : str
            Path to the scrape log

        Returns
        -------
        success : str
            True if any data does not exist after attempting deletion, False if it does

        """

        click.echo(f'Deleting data associated with the scrape job: {scrape_label} ...')

        utils.delete_directory(download_directory)
        utils.delete_file(zip_file)
        utils.delete_file(log)

        if (
            not utils.directory_exists(download_directory) and
            not utils.file_exists(zip_file) and
            not utils.file_exists(log)
            ):

            success = True
            click.echo(f'Data associated with {scrape_label} deleted.\n')

        else:
            success = False
            click.echo(f'Failed to delete all data associated with {scrape_label}.\n')

        return success

    def clean_parse_job_data(self, parse_label, parse_directory, log):
        """
        Deletes all data associated with a parse job.

        Parameters
        ----------
        parse_label : str
            Label of the parse job whose data is being deleted
        parse_directory : str
            Path to the directory where the data was downloaded
        log : str
            Path to the parse log

        Returns
        -------
        success : str
            True if any data does not exist after attempting deletion, False if it does

        """

        click.echo(f'Deleting data associated with the parse job: {parse_label} ...')

        utils.delete_directory(parse_directory)
        utils.delete_file(log)

        if (
            not utils.directory_exists(parse_directory) and
            not utils.file_exists(log)
            ):

            success = True
            click.echo(f'Data associated with {parse_label} deleted.\n')

        else:
            success = False
            click.echo(f'Failed to delete all data associated with {parse_label}.\n')

        return success
