import os
import platformdirs
import click

import appeer.utils
import appeer.log

from appeer.config import Config

class Datadir:
    """
    Class which handles creation, deletion and checking of the
    ``appeer`` data directory, which is found at ``platformdirs.user_data_dir(appname='appeer')``.
    """

    def __init__(self):
        """
        Store default directories using ``platformdirs``.
        """

        config = Config()

        self.base = config._base_directory
        
        self.downloads = f'{self.base}/downloads'
        
        self.scrape_archives = f'{self.base}/scrape'
        self.scrape_logs = f'{self.base}/scrape/scrape_logs'

        self.parse = f'{self.base}/parse'
        self.parse_logs = f'{self.base}/parse/parse_logs'

        self.db = f'{self.base}/db'

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

        click.echo(self._dashes)

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
