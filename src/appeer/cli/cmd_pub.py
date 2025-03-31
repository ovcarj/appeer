"""Defines the ``appeer pub`` CLI"""

import click

from appeer.pub import status

@click.group('pub', invoke_without_command=True,
        help="""*** Analyze publications ***

        (*) Simple alphabetical list of publishers:

                appeer pub -P

        (*) Simple alphabetical list of journals for a given publisher:

                appeer pub -J -p 'Nature Porfolio'

""", short_help='Analyze publications')
@click.option('-P', '--publisher_list', is_flag=True, default=False)
@click.option('-J', '--journal_list', is_flag=True, default=False)
@click.option('-p', '--publisher')
@click.pass_context
def pub_cli(ctx, **kwargs):
    """
    Pub CLI

    """

    if ctx.invoked_subcommand is None:

        if kwargs['publisher_list']:
            click.echo(status.unique_publishers_report())

        elif kwargs['journal_list']:

            if not kwargs['publisher']:
                click.echo('A publisher must be provided, e.g. appeer pub -J -p "Nature Portfolio"')

            else:
                click.echo(status.unique_journals_report(
                    publisher=kwargs['publisher']))
