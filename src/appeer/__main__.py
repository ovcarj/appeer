__version__ = '0.0.1'

import click
from appeer.cmdline import cmd_clean, cmd_init, cmd_scrape, cmd_parse, cmd_config, cmd_sdb

@click.group(name='appeer')
def appeer_cli():
    pass

appeer_cli.add_command(cmd_scrape.scrape_cli, name='scrape')
appeer_cli.add_command(cmd_clean.clean_cli, name='clean')
appeer_cli.add_command(cmd_init.init_cli, name='init')
appeer_cli.add_command(cmd_config.config_cli, name='config')
appeer_cli.add_command(cmd_sdb.sdb_cli, name='sdb')
appeer_cli.add_command(cmd_parse.parse_cli, name='parse')
