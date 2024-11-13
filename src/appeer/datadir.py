import os
import platformdirs
import click

import appeer.utils
import appeer.log

from appeer.config import Config

class Datadir:
    """
    Handles creation, deletion and checking of the
    ``appeer`` data directory, which is, by default, 
    found at ``platformdirs.user_data_dir(appname='appeer')``.

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

        self._dashes = appeer.log.get_log_dashes()

    def check_existence(self):
        """
        Checks the existence of the ``self.base`` directory; 
        the ``self._base_exists`` attribute is updated accordingly.
        """

        self._base_exists = appeer.utils.directory_exists(self.base)

    def create_directories(self):
        """
        Creates ``appeer`` data directories. If ``self.base`` already exists, 
        the user is prompted if they want to overwrite the directory.

        """

        input_ok = False
        
        if self._base_exists:

            overwrite = appeer.log.ask_yes_no(f'WARNING: appeer data base directory exists at {self.base}\nDo you want to overwrite it? All data will be deleted. [Y/n]\n')

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
            appeer.utils.delete_directory(self.base)

        self.check_existence()

    def clean_downloads(self):
        """
        Deletes the contents of ``self.downloads``

        """

        appeer.utils.delete_directory_content(self.downloads)

    def clean_scrape_archives(self):
        """
        Deletes the files in ``self.scrape_archives``

        """

        appeer.utils.delete_directory_files(self.scrape_archives)

    def clean_scrape_logs(self):
        """
        Deletes the files in ``self.scrape_logs``

        """

        appeer.utils.delete_directory_files(self.scrape_logs)

    def clean_parse(self):
        """
        Deletes the files in ``self.parse``

        """

        appeer.utils.delete_directory_files(self.parse)

    def clean_parse_logs(self):
        """
        Deletes the files in ``self.parse_logs``

        """

        appeer.utils.delete_directory_files(self.parse_logs)

    def clean_db(self):
        """
        Deletes the files in ``self.db``

        """

        appeer.utils.delete_directory_files(self.db)

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

        click.echo(self._dashes)
        click.echo(f'Deleting data associated with the scrape job: {scrape_label}')
        click.echo(self._dashes)

        appeer.utils.delete_directory(download_directory)
        appeer.utils.delete_file(zip_file)
        appeer.utils.delete_file(log)

        click.echo(self._dashes)

        if (
            not appeer.utils.directory_exists(download_directory) and
            not appeer.utils.file_exists(zip_file) and
            not appeer.utils.file_exists(log)
            ):

            success = True
            click.echo(f'Data associated with {scrape_label} deleted.')

        else:
            success = False
            click.echo(f'Failed to delete all data associated with {scrape_label}.')

        return success
