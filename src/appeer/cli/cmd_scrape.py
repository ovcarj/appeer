"""
Defines the ``appeer scrape`` CLI
"""

import click

from appeer.scrape.scrape import main as scrape_main
from appeer.scrape.scrape_plan import main as scrape_planning
from appeer.general.config import Config

settings = Config().settings

if settings:

    default_sleep_time = float(settings['ScrapeDefaults']['sleep_time'])
    default_max_tries = int(settings['ScrapeDefaults']['max_tries'])
    default_retry_sleep_time = float(settings['ScrapeDefaults']['retry_sleep_time'])

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
@click.option('-o', '--output', 'output_zip_filename',
        help="Name of the ZIP archive containing the downloaded data. If not given, a default name based on the timestamp is generated")
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
@click.option('-l', '--logdir',
        default=None,
        help="Directory in which to store the log. If not given, the default appeer data directory is used")
@click.option('-d', '--download_dir',
        default=None,
        help="Directory into which to download the files. If not given, the default appeer data directory is used")
@click.option('-c', '--cleanup',
        is_flag=True, default=False,
        help="Delete the directory with the downloaded data after scrape")
@click.option('-p','--preview',
        is_flag=True, default=False,
        help="Get a preview of the scraping strategy for a given input file")
def scrape_cli(filename, output_zip_filename,
        description,
        sleep_time, max_tries, retry_sleep_time,
        logdir, download_dir,
        cleanup,
        preview):
    """
    Scrape publications

    """

    publications = filename

    if preview:
        scrape_planning(publications)

    else:
        scrape_main(publications, output_zip_filename,
            description,
            sleep_time, max_tries, retry_sleep_time,
            logdir, download_dir,
            cleanup)
