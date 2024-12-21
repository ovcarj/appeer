"""Defines the ``appeer scrape`` CLI"""

import click

from appeer.scrape import scrape_scripts
from appeer.scrape.strategies.scrape_plan import preview_plan
from appeer.general.config import Config

settings = Config().settings

if settings:

    default_sleep_time = float(
            settings['ScrapeDefaults']['sleep_time'])
    default_max_tries = int(
            settings['ScrapeDefaults']['max_tries'])
    default_retry_sleep_time = float(
            settings['ScrapeDefaults']['retry_sleep_time'])

else:
    # most probably "appeer init" was not yet run
    default_sleep_time = 1.0
    default_max_tries = 3
    default_retry_sleep_time = 10.0

@click.command(help="""Download publications data for later parsing

Example usage: appeer scrape -c -s "My scrape description" PoP.json

The input file can be either a JSON file (e.g. a PoP.json file containing 'article_url' keys) or a plaintext file with each URL written in a new line.

The URLs can be written either as a full URL or a DOI.
E.g., the following entries are equally valid:

https://pubs.rsc.org/en/content/articlelanding/2023/ob/d3ob00424d
10.1039/D3OB00424D

Valid entries start with 'https://' or need to be in DOI format (10.prefix/suffix).

If the entry format is invalid, the invalid URL is not scraped.
""")
@click.argument('filename')
@click.option('-o', '--output', 'zip_file',
        help="Name of the ZIP archive containing the downloaded data")
@click.option('-s', '--description', 'description',
        default=None,
        help="Optional description of the scrape job")
@click.option('-t', '--sleep_time',
        default=default_sleep_time, show_default=True,
        help="Time (in seconds) between sending requests")
@click.option('-m', '--max_tries',
        default=default_max_tries, show_default=True,
        help="Maximum number of tries to get a response from an URL before giving up")
@click.option('-rt', '--retry_sleep_time',
        default=default_retry_sleep_time, show_default=True,
        help="Time (in seconds) between retrying a URL")
@click.option('-l', '--log_directory',
        default=None,
        help="Directory in which to store the log")
@click.option('-d', '--download_directory',
        default=None,
        help="Directory into which to download the files")
@click.option('-c', '--cleanup',
        is_flag=True, default=False,
        help="Delete the directory containing the downloaded data after job ends")
@click.option('-j', '--job_label',
        default=None,
        help="Scrape job label")
@click.option('-p','--preview',
        is_flag=True, default=False,
        help="Get a preview of the scraping strategy for a given input file")
def scrape_cli(filename, **kwargs):
    """
    Scrape CLI

    """

    publications = filename

    if kwargs['preview']:
        preview_plan(publications)

    else:
        scrape_scripts.create_and_run(publications=publications,
                                      label=kwargs['job_label'],
                                      **kwargs)
