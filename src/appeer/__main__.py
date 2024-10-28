__version__ = '0.0.1'

import click
from appeer.cmdline import cmd_clean, cmd_init, cmd_scrape, cmd_config

@click.group(name='appeer')
def appeer_cli():
    pass

appeer_cli.add_command(cmd_scrape.scrape_cli, name='scrape')
appeer_cli.add_command(cmd_clean.clean_cli, name='clean')
appeer_cli.add_command(cmd_init.init_cli, name='init')
appeer_cli.add_command(cmd_config.config_cli, name='config')
