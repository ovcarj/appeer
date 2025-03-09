"""Defines the ``appeer parse`` CLI"""

import sys

import click

from appeer.parse import parse_scripts

@click.command(help="""*** Parse publications ***

    (*) The most common usage is to find and parse publications automatically:

            appeer parse

    (*) Examples of advanced usage:

            appeer parse -c -x -p RSC -p NAT

            appeer parse -E -s "Parse job description"

            appeer parse -F file1.html file2.xml

            appeer parse -F *

            appeer parse -S scrape_label1 scrape_label2

    (*) Parsing can be performed in one of 4 modes, defined by the (-A, -E, -S, -F flags):

        (1) Mode A [DEFAULT]

            Searches the scrape jobs database and parses previously unparsed publications

                appeer parse

        (2) Mode E

            Parses all publications found in the scrape jobs database, regardless of whether they were previously parsed

                appeer parse -E

        (3) Mode S

            Parses publications from a list of scrape job labels

                appeer parse -S scrape_label1 scrape_label2

        (4) Mode F

            Parses a list of files (independent of the scrape jobs database)

                appeer parse -F file1.html file2.xml

                appeer parse -F *

    (*) For each publication, the publisher and journal are determined automatically.
    It is possible to accelerate this process by restricting the list of candidate parsers
    by specifying the (-p, -r, -d) options:

        appeer parse -p RSC -r ANY -d txt

    (*) Several other options and flags are available. The most relevant ones are:

        (1) -c flag: If a temporary directory for parsing was created, delete it upon completion of the parse job

        (2) -x flag: DO NOT mark scrape jobs as parsed upon parse job completion, even if they were successfully parsed


""")
@click.argument('inputs', nargs=-1)
@click.option('-A', '--automatic', 'mode', flag_value='A', default=True,
        help="Parse mode A [default]")
@click.option('-E', '--everything', 'mode', flag_value='E',
        help='Parse mode E')
@click.option('-S', '--scrape_jobs', 'mode', flag_value='S',
        help='Parse mode S')
@click.option('-F', '--file_list', 'mode', flag_value='F',
        help='Parse mode F')
@click.option('-p', '--publishers', multiple=True,
        help="Candidate parser")
@click.option('-r', '--journals', multiple=True,
        help="Candidate journal")
@click.option('-d', '--data_types', multiple=True,
        help="Candidate data type")
@click.option('-x', '--no_scrape_mark',
        is_flag=True, default=False,
        help="Do not mark scrape jobs as parsed")
@click.option('-s', '--description', 'description',
        help="Optional description of the parse job")
@click.option('-l', '--log_directory',
        type=click.Path(file_okay=False, writable=True),
        help="Directory in which to store the log")
@click.option('-t', '--tmp_directory',
        type=click.Path(file_okay=False, writable=True),
        help="Directory into which to unpack files")
@click.option('-c', '--cleanup',
        is_flag=True, default=False,
        help="Delete the temporary directory after job ends")
@click.option('-j', '--job_label',
        help="Parse job label")
def parse_cli(inputs, **kwargs):
    """
    Parse CLI

    """

    data_source = list(inputs) or None

    if (data_source and kwargs['mode'] in ('A', 'E')):
        click.echo('Error: No inputs should be provided for parse modes "A" and "E".')
        sys.exit()

    if (not data_source and kwargs['mode'] in ('S', 'F')):
        click.echo('Error: Inputs must be provided for parse modes "S" and "F".')
        sys.exit()

    kwargs['publishers'] = list(kwargs['publishers']) or None
    kwargs['journals'] = list(kwargs['journals']) or None
    kwargs['data_types'] = list(kwargs['data_types']) or None

    if (kwargs['data_types'] and kwargs['data_types'] not in ('txt', ['txt'])):
        click.echo('Error: Cannot start parsing; only data_type="txt" is currently supported.')
        sys.exit()

    parse_scripts.create_and_run(
            data_source=data_source,
            label=kwargs['job_label'],
            **kwargs)
