import click

from appeer.config import Config

@click.command('print', help='Print the ``appeer`` config file.')
def show_config():
    """
    Prints the ``appeer`` config file.

    """

    cfg = Config()
    cfg.print_config()

@click.command('edit', help="""Edit the contents of the config file.

        Example usage: appeer config edit data_directory /home/user/appeer_data

        The argument after "edit" should be one of the subsections of the config file.

        To see the current config file, use:

        appeer config print

        """)
@click.argument('option')
@click.argument('value', nargs=-1)
def edit_config(option, value):
    """
    Edit the ``appeer`` config file by passing a subection and the new value

    """

    value_str = ' '.join(value)

    cfg = Config()
    cfg.edit_config_by_subsection(subsection=option, value=value_str)

@click.group()
def config_cli(name='config', help='Print/edit the ``appeer`` config file'):
    """
    Print/edit the ``appeer`` config file

    """
    pass

config_cli.add_command(show_config)
config_cli.add_command(edit_config)

def main():
    config_cli()

if __name__ == '__main__':
    main()