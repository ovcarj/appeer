import click

from appeer.parse.parse_automatic import parse_automatic

@click.command(help="""Parse downloaded publications

Example usage: appeer parse

""")
@click.option('-m', '--mode', 'mode', default='auto', show_default=True, help="Parsing mode")
@click.option('-s', '--description', 'description', default=None, help="Optional description of the parse job")
@click.option('-l', '--logdir', default=None, help="Directory in which to store the log. If not given, the default appeer data directory is used")
@click.option('-d', '--parse_dir', default=None, help="Directory into which to create files for parsing. If not given, the default appeer data directory is used")
@click.option('-k', '--keep_tmp', is_flag=True, default=False, help="Don't delete the temporary files after parsing")
@click.option('-c', '--commit', is_flag=True, default=False, help="After parsing, commit the results to the publications database")
def parse_cli(mode,
        description,
        logdir,
        parse_dir,
        commit,
        keep_tmp):

    cleanup = not keep_tmp

    if mode == 'auto':
        parse_automatic(description=description,
                logdir=logdir, parse_directory=parse_dir,
                commit=commit, cleanup=cleanup)

    else:
        click.echo(f'Parsing mode "{mode}" not implemented')

if __name__ == '__main__':
    parse_cli()
