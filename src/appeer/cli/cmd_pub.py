"""Defines the ``appeer pub`` CLI"""

import click

from appeer.pub import status

@click.group('pub', invoke_without_command=True,
        help="""*** Analyze publications ***

        (*) List publishers alphabetically:

                appeer pub -P

""", short_help='Analyze publications')
@click.option('-P', '--publisher_list', is_flag=True, default=False)
@click.pass_context
def pub_cli(ctx, publisher_list):
    """
    Pub CLI

    """

    if ctx.invoked_subcommand is None:

        if publisher_list:
            click.echo(status.unique_publishers_report())
