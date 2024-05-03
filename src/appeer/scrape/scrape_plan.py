import sys
import click

import appeer.utils
import appeer.log

from appeer.scrape.input_handling import parse_data_source, handle_input_reading

class ScrapePlan:
    """
    Parse URL list to understand which publisher it corresponds to 
    and get the corresponding scrape strategies.
    """

    def __init__(self, url_list):
        """
        Initialize the ``ScrapePlan`` object.

        Parameters
        ----------
        url_list : list
            URL list of strings starting with https://
        """

        self._define_publisher_codes()
        self._define_strategy_codes()

        # Special case of one URL
        if isinstance (url_list, str):
            url_list = [url_list]

        self.url_list = url_list

        self._understand_publishers()
        self._count_publishers()

        self._get_strategies()
        self._count_strategies()

        self._get_strategy_report()

    def _define_publisher_codes(self):
        """
        Define a dictionary of internal publisher codes, 
        depending on how the URL string after ``'https://'`` starts.

        If the URL does not start with ``'https://'``, ``skip`` code is generated,
        so the URL is skipped during scraping.
        """

        self._publisher_codes = {
                'invalid_url': 'Invalid URL',
                'unknown': 'Unknown',
                'doi.org': 'DOI',
                'pubs.rsc.org': 'RSC'
                }

    def _define_strategy_codes(self):
        """
        Define a dictionary of strategy codes depending on 
        the internal publisher codes.

        Strategies:
        ----------
        skip
            will not scrape the URL
        html
            scrape HTML
        doi
            resolve DOI to update the strategy and act accordingly

        """

        self._strategy_codes = {
                'Invalid URL': 'skip',
                'Unknown': 'html',
                'DOI': 'doi',
                'RSC': 'html'
                }

    def _understand_publishers(self):
        """
        Parse URL list to understand which publisher it corresponds to.
        The list of publishers is stored to self.publishers.
        """
    
        url_list = self.url_list
        publishers = []
    
        for url in url_list:

            publisher = None
    
            if not url.startswith('https://'):
                publisher = self._publisher_codes['invalid_url']
        
            else:
        
                url_split = url.split('https://')[1]

                for url_start, code in self._publisher_codes.items():

                    if url_split.startswith(url_start):
                        publisher = code

                if not publisher:
                    publisher = self._publisher_codes['unknown']
   
            publishers.append(publisher)
    
        self.publishers = publishers

    def _get_strategies(self):
        """
        Get scrape strategies from a list of publisher internal codes.
        The list of strategies is stored to self.strategies.
        """
    
        strategies = []

        for publisher in self.publishers:

            try:
                strategy = self._strategy_codes[publisher]

            except KeyError:
                strategy = 'skip'

            strategies.append(strategy)

        self.strategies = strategies

    def _count_publishers(self):
        """
        Count how many different types of publishers are found 
        and store the results to self._publishers_count
        """

        publishers_count = {}

        for code in self._publisher_codes.values():
            publishers_count[code] = self.publishers.count(code)

        self._publishers_count = publishers_count

    def _count_strategies(self):
        """
        Count how many different types of strategies are found 
        and store the results to self._strategies_count
        """

        strategies_count = {}

        for code in self._strategy_codes.values():
            strategies_count[code] = self.strategies.count(code)

        self._strategies_count = strategies_count

    def _get_strategy_report(self):
        """
        Get a preview of strategies on how publications data would be downloaded.
        """
    
        strategy_report = ''
        dashes = appeer.log.get_log_dashes()
        short_dashes = appeer.log.get_short_log_dashes()
    
        strategy_report += f'SCRAPE PLAN SUMMARY\n\n'

        no_of_urls = len(self.url_list)
    
        strategy_report += f'Number of inputted URLs: {no_of_urls}\n'
        strategy_report += '\n'
    
        strategy_report += f'Deduced publishers from URLs:\n'
    
        for publisher, count in self._publishers_count.items():
    
            if count > 0:
                strategy_report += f'{publisher}: {count}/{no_of_urls}\n'
    
        strategy_report += '\n'
    
        strategy_report += f'Planned strategies:\n'
    
        for strategy, count in self._strategies_count.items():
    
            if count > 0:
                strategy_report += f'{strategy}: {count}/{no_of_urls}\n'
    
        strategy_report += short_dashes + '\n'
        strategy_report += f'DETAILED SCRAPE PLAN (url publisher strategy)\n\n'
        
        for i, url in enumerate(self.url_list):
            strategy_report += f'{url} {self.publishers[i]} {self.strategies[i]}\n'
    
        self._strategy_report = strategy_report

def main(publications):
    """
    Get the download strategy for a list of publications.

    ``publications`` can be a list of URLs or a string (path to a file).
    In the case a filepath is provided, the file can be either a JSON file
    (e.g. ``PoP.json`` file containing ``['article_url']`` keys)
    or a plaintext file with each URL in a new line.
    """

    start_datetime = appeer.utils.get_current_datetime()
    start_report = appeer.log.appeer_start(start_datetime)
    click.echo(start_report)

    data_source, data_source_type, plaintext_ex_message, json_ex_message = parse_data_source(publications)
    reading_passed, reading_report = handle_input_reading(publications, data_source_type, str(plaintext_ex_message), str(json_ex_message))

    click.echo(reading_report)

    if not reading_passed:
        end_report = appeer.log.appeer_end(start_datetime=start_datetime)
        click.echo(end_report)
        sys.exit()
    
    plan = ScrapePlan(data_source)

    strategy_report = plan._strategy_report
    click.echo(strategy_report)

    end_report = appeer.log.appeer_end(start_datetime)
    click.echo(end_report)

if __name__ == '__main__':
    main(sys.argv[1])
