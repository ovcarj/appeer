import click

from appeer.scrape.scrape import main as scrape_main
from appeer.scrape.scrape_plan import main as scrape_planning

@click.command(help="""Download publications data for later parsing.

Example usage: appeer scrape -c PoP.json

The input file can be either a JSON file (e.g. a PoP.json file containing 'article_url' keys) or a plaintext file with each URL written in a new line.

The URLs can be written either as a full URL or a DOI.
E.g., the following entries are equally valid:

https://pubs.rsc.org/en/content/articlelanding/2023/ob/d3ob00424d
10.1039/D3OB00424D

Valid entries start with 'https://' or need to be in DOI format (10.prefix/suffix).

If the entry format is invalid, the invalid URL is not scraped.
""")
@click.argument('filename')
@click.option('-o', '--output', 'output_zip_filename', help="Name of the ZIP archive containing the downloaded data. If not given, a default name based on the timestamp is generated")
@click.option('-t', '--sleep_time', default=1.0, show_default=True, help="Time (in seconds) between sending requests")
@click.option('-m', '--max_tries', default=3, show_default=True, help="Maximum number of tries to get a response from an URL before giving up")
@click.option('-rt', '--retry_sleep_time', default=10.0, show_default=True, help="Time (in seconds) between retrying a URL")
@click.option('-l', '--logdir', default=None, help="Directory in which to store the logfile. If not given, default ``appeer`` data directory is used (recommended)")
@click.option('-d', '--download_dir', default=None, help="Directory into which to download the files. If not given, the default ``appeer`` data directory is used")
@click.option('-c', '--cleanup', is_flag=True, default=False, help="Delete the directory with the downloaded data upon successful completion (output ZIP archive is kept)")
@click.option('-p','--preview', is_flag=True, default=False, help="Get a preview of the scraping strategy for a given input file (no scraping is done)")
def scrape_cli(filename, output_zip_filename,
        sleep_time, max_tries, retry_sleep_time,
        logdir, download_dir,
        cleanup,
        preview):

    publications = filename

    if preview:
        scrape_planning(publications)

    else:
        scrape_main(publications, output_zip_filename, 
            sleep_time, max_tries, retry_sleep_time,
            logdir, download_dir,
            cleanup)

if __name__ == '__main__':
    scrape_cli()
