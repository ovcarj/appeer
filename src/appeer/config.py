import sys
import os
import click

import appeer.utils
import appeer.log

import platformdirs
import configparser

class Config:
    """
    Class which handles creation, deletion, reading and editing of the
    ``appeer`` config file, which is found at 
    ``platformdirs.user_config_dir(appname='appeer')/appeer.cfg``.
    """

    def __init__(self):
        """
        Check for the existence of the config file. If it exists, read its contents.
        """

        self._dashes = appeer.log.get_log_dashes()

        self._config = configparser.ConfigParser()

        self._config_dir = platformdirs.user_config_dir(appname='appeer')
        self._config_path = f'{self._config_dir}/appeer.cfg'

        self.check_existence()

        if self._config_exists:
            self.read_config()

    def check_existence(self):
        """
        Checks the existence of the ``self.config`` directory; 
        the ``self._config_exists`` attribute is updated accordingly.
        """

        self._config_exists = appeer.utils.file_exists(self._config_path)

    def define_default_values(self):
        """
        Defines the default values for the appeer config file.
        """

        default_base = platformdirs.user_data_dir(appname='appeer')

        self._config['appeerDataDirectory'] = {'data_directory': default_base}

    def create_config_file(self):
        """
        Creates the ``appeer`` config file. If ``self._config_path`` already exists, 
        the user is prompted if they want to proceed with the current config file.
        """

        self.check_existence()

        if self._config_exists:

            click.echo(f'WARNING: appeer config file already exists at {self._config_path}')
            click.echo(self._dashes)
            self.handle_config_exists()

        else:

            self.define_default_values()

            if not appeer.utils.directory_exists(self._config_dir):
                os.makedirs(self._config_dir)

            try:
                with open(self._config_path, 'w+') as configfile:
                    self._config.write(configfile)

            except:
                click.echo('Failed to initialize the appeer config file at {self._config_path}. Exiting.')
                sys.exit()

            self.check_existence()

            if self._config_exists:

                click.echo(f'Path to the appeer configuration file: {self._config_path}')
                click.echo(self._dashes)
                self.read_config()

            else:
                click.echo('Failed to initialize the appeer config file at {self._config_path}. Exiting.')
                sys.exit()

            click.echo(f'appeer will store all data in a given base directory.')
            click.echo(f'The default directory is: {self._base_directory}\n')

            new_path = input(f'Press "Enter" to keep the default or provide another path:\n')

            if len(new_path) > 0:

                self.edit_config_file(update_dict={'appeerDataDirectory': {'data_directory': new_path}})


            click.echo(self._dashes)
            click.echo(f'appeer config successful!')
            click.echo(self._dashes)

            self.print_config()

    def handle_config_exists(self):
        """
        Handles the case when the user tries to run ``appeer init`` with a preexisting config file.
        """
        
        self.print_config()

        proceed = appeer.log.ask_yes_no('Do you want to proceed with the current config file? [Y/n]\n')
        
        if proceed == 'Y':

            click.echo('Proceeding with the current appeer config file.')
            click.echo(self._dashes)

        elif proceed == 'n':

            click.echo(f'Stopping, as requested.')
            click.echo(f'Check "appeer config --help" for instructions on editing the config file or run "appeer clean config" to delete the config file.')
            click.echo(self._dashes)

            sys.exit()

    def read_config(self):
        """
        Reads the contents of the config file and stores the values to ``self._config``.
        """

        self.check_existence()

        if self._config_exists:

            self._config.read(self._config_path)

            self._base_directory = self._config['appeerDataDirectory']['data_directory']

        else:
            click.echo('The appeer config file does not exist.')

    def print_config(self):
        """
        Prints the contents of the config file.
        """

        self.check_existence()

        if self._config_exists:

            with open(self._config_path, 'r') as f:

                click.echo(f'Contents of the appeer config file at {self._config_path}:')
                click.echo(self._dashes)
                click.echo(f.read(), nl=False)

                click.echo(self._dashes)

        else:
            click.echo('The appeer config file does not exist.')

    def edit_config_file(self, update_dict):
        """
        Edit the contents of the config file.

        The update_dict should be of form {section0: {subsection00: value00, subsection01: value01}, section1: {subsection10: value10, subsection11: value11}, ...},
        where the sections and subsections correspond to the configuration file.

        Parameters
        ----------
        update_dict : dict
            Dictionary of form {section0: {subsection00: value00, subsection01: value01}, section1: {subsection10: value10, subsection12: value12}, ...}

        """

        self.read_config()

        all_sections = self._config.sections()

        for section, subsections_values in update_dict.items():

            if section not in all_sections:
                click.echo(f'Invalid configuration section {section}')

            else:

                all_subsections = self._config[section].keys()

                for subsection, value in subsections_values.items():

                    if subsection not in all_subsections:
                        click.echo(f'Invalid configuration subsection {subsection}')

                    else:

                        if (section == 'appeerDataDirectory' and subsection == 'data_directory'):

                            editing_datadir = True

                            click.echo('WARNING: You are attempting to edit the path to the base appeer data directory. If the directory was not previously initialized, you will have to rerun "appeer init".')
                            proceed = appeer.log.ask_yes_no('Do you wish to proceed?\n')

                            if proceed == 'n':
                                click.echo('Stopping, as requested.')
                                return

                            else:
                                pass

                        self._config[section][subsection] = value
                        
                        with open(self._config_path, 'w') as configfile:
                            self._config.write(configfile)

                        click.echo(f'[{section}]: {subsection} updated to {value}')

                        if editing_datadir:
                            click.echo('You have edited the path to the base appeer data directory. If necessary, rerun "appeer init".')

                        self.read_config()

    def edit_config_by_subsection(self, subsection, value):
        """
        Edits the contents of the config file by passing only the subsection and the value, while the section is automatically found.

        Parameters
        ----------
        subsection : str
            Subsection in the config file
        value : str | float | int
            Value that the subsection is updated to

        """

        self.read_config()
        all_sections = self._config.sections()

        update_dict = {}

        for section in all_sections:

            if subsection in self._config.options(section):

                update_dict[section] = {subsection: value}
                self.edit_config_file(update_dict=update_dict)

                break
            
        else:
            click.echo(f'Invalid config option "{subsection}"')

    def clean_config(self):
        """
        Deletes the ``self._config_dir`` configuration directory.
        """

        if not appeer.utils.directory_exists(self._config_dir):
            click.echo('Nothing to delete.')

        else:
            appeer.utils.delete_directory(self._config_dir)
