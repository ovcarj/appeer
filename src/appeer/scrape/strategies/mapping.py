"""Map web domains to scraping strategies"""

import os

from collections import namedtuple

from appeer.general import utils

# Defined in DSM.json
DomainScrapeMap = namedtuple('DomainScrapeMap', [
    'web_domain',           # Unique web domain
    'journal_code',         # Internal publisher code
    'strategy_code',        # Internal strategy code
    ])

# Defined in SMM.json
ScrapeMethodMap = namedtuple('ScrapeMethodMap', [
    'strategy_code',        # Internal strategy code
    'scraping_method',      # Name of the scraping method
    ])

class JournalScrapeMap:
    """
    Map domains to scraping strategies
    
    """

    def __init__(self):
        """
        Load "DSM.json" and "SMM.json"

        """

        strategy_path = os.path.dirname(__file__)
        dsm_path = os.path.join(strategy_path, 'DSM.json')
        smm_path = os.path.join(strategy_path, 'SMM.json')

        dsm = [DomainScrapeMap(**d)
                for d in utils.load_json(dsm_path)['DSM']]

        smm = [ScrapeMethodMap(**d)
                for d in utils.load_json(smm_path)['SMM']]

        self.strategy_map = {}

        for domain in dsm:

            strategy_code = domain.strategy_code

            for method in smm:

                if strategy_code == method.strategy_code:
                    scraping_method = method.scraping_method
                    break

            else:
                raise ValueError(f'Invalid strategy_code "{strategy_code}" given; could not find the appropriate scraping method')

            self.strategy_map[f'{domain.web_domain}'] = {
                    'journal_code': domain.journal_code,
                    'strategy_code': domain.strategy_code,
                    'scraping_method': scraping_method
                    }

    def get_strategy(self, domain):
        """
        Get the journal and strategy codes and scraping method of a domain

        If the domain is not defined in ``DSM.json``, treat it as an
            unknown domain (will be skipped)

        Parameters
        ----------
        domain : str
            Web domain

        Returns
        -------
        strategy : dict
            Dictionary with ``journal_code``, ``strategy_code`` and
                ``scraping_method`` keys

        """

        try:
            strategy = self.strategy_map[domain]

        except KeyError:
            strategy = self.strategy_map['unknown']

        return strategy
