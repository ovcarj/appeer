"""Defines the ``appeer clean`` CLI"""

import sys
import click

from appeer.general import log

from appeer.general.datadir import Datadir
from appeer.general.config import Config
from appeer.scrape import clean_scrape_jobs as csj
from appeer.parse import clean_parse_jobs as cpj

@click.command('all_data', help='Delete the appeer directory')
def clean_all_data():
    """
    Deletes all ``appeer`` data directories

    """

    ad = Datadir()
    ad.clean_all_directories()

@click.command('downloads',
        help='Delete contents of the appeer/downloads directory')
def clean_downloads():
    """
    Deletes the contents of the ``appeer/downloads`` directory

    """

    ad = Datadir()
    ad.clean_downloads()

@click.command('scrape_archives',
        help='Delete contents of the appeer/scrape_archives directory')
def clean_scrape_archives():
    """
    Deletes the contents of the ``appeer/scrape_archives`` directory

    """

    ad = Datadir()
    ad.clean_scrape_archives()

@click.command('scrape_logs',
        help='Delete contents of the appeer/scrape_logs directory')
def clean_scrape_logs():
    """
    Deletes the contents of the ``appeer/scrape_logs`` directory

    """

    ad = Datadir()
    ad.clean_scrape_logs()

@click.command('parse',
        help='Delete contents of the appeer/parse directory')
def clean_parse():
    """
    Deletes the contents of the ``appeer/parse`` directory

    """

    ad = Datadir()
    ad.clean_parse()

@click.command('parse_logs',
        help='Delete contents of the appeer/parse_logs directory')
def clean_parse_logs():
    """
    Deletes the contents of the ``appeer/parse_logs`` directory

    """

    ad = Datadir()
    ad.clean_parse_logs()

@click.command('db',
        help='Delete contents of the appeer/db directory')
def clean_db():
    """
    Deletes the contents of the ``appeer/db`` directory

    """

    ad = Datadir()
    ad.clean_db()

@click.command('config',
        help='Delete the appeer config file')
def clean_config():
    """
    Deletes the ``appeer`` config file

    """

    cfg = Config()
    cfg.clean_config()

@click.command('sjob', help="""Delete scrape job(s) data

        Delete a single job with a given job label:

            appeer clean sjob scrape_20241113-070355_8

        Delete all jobs whose status is not 'X'

            appeer clean sjob --bad

        Delete all jobs
        
            appeer clean sjob --all

        View a summary of all scrape jobs, type:

            appeer sjob

        """)
@click.argument('label', nargs=1, required=False)
@click.option('-b', '--bad', is_flag=True,
        default=False, help='Delete jobs whose status is not X')
@click.option('-a', '--all', 'everything', is_flag=True,
        default=False, help='Delete all scrape jobs')
def clean_sjob(label, bad, everything):
    """
    Delete data associated with a scrape job with a given ``label``

    """

    if bad:
        csj.clean_bad_jobs()

    elif everything:
        proceed = log.ask_yes_no('You are about to delete all scrape jobs data. Do you want to proceed? [Y/n]\n')

        if proceed == 'Y':
            csj.clean_all_jobs()

        elif proceed == 'n':

            click.echo('Stopping.')
            sys.exit()

    else:
        csj.clean_scrape_job(label)

@click.command('pjob', help="""Delete parse job(s) data

        Delete a single job with a given label:

            appeer clean sjob parse_20241113-070355_8

        Delete all jobs whose status is not 'X':

            appeer clean pjob --bad

        Delete all jobs:
        
            appeer clean pjob --all

        View a summary of all parse jobs:

            appeer pjob

        """)
@click.argument('label', nargs=1, required=False)
@click.option('-b', '--bad', is_flag=True,
        default=False, help='Delete jobs whose status is not X')
@click.option('-a', '--all', 'everything', is_flag=True,
        default=False, help='Delete all parse jobs')
def clean_pjob(label, bad, everything):
    """
    Delete data associated with a parse job with a given ``label``

    """

    if bad:
        cpj.clean_bad_jobs()

    elif everything:
        proceed = log.ask_yes_no('You are about to delete all parse jobs data. Do you want to proceed? [Y/n]\n')

        if proceed == 'Y':
            cpj.clean_all_jobs()

        elif proceed == 'n':

            click.echo('Stopping.')
            sys.exit()

    else:
        cpj.clean_parse_job(label)

@click.group()
def clean_cli(name='clean', help='Delete contents of the appeer data directory'): #pylint:disable=unused-argument, redefined-builtin, line-too-long
    """
    Delete contents of the appeer data directory

    Example usage:

        appeer clean downloads

        appeer clean sjob my_scrape_label

    """

clean_cli.add_command(clean_all_data)
clean_cli.add_command(clean_downloads)
clean_cli.add_command(clean_scrape_archives)
clean_cli.add_command(clean_scrape_logs)
clean_cli.add_command(clean_parse)
clean_cli.add_command(clean_parse_logs)
clean_cli.add_command(clean_db)

clean_cli.add_command(clean_config)

clean_cli.add_command(clean_sjob)
clean_cli.add_command(clean_pjob)
