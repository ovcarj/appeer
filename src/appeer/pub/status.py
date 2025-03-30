"""Get reports on the current status of publisher/journal entries in pub.db"""

from collections import defaultdict

from appeer.general import log as _log

from appeer.db.pub_db import PubDB

def group_by_alphabet(entry_list):
    """
    Groups a list of strings by alphabet

    E.g., given a list:

        entry_list = ['Alpha1', 'alpha2', 'beta1', 'Beta2']

    The result is of format:

        alphabet_dict = {
            'A': ['Alpha1', 'alpha2'],
            'B': ['beta1', 'Beta2']
            }

    Parameters
    ----------
    entry_list : list of str
        List of strings to be grouped by alphabet

    Returns
    -------
    alphabet_dict : defaultdict(list)
        Dictionary of grouped strings grouped by alphabet

    """

    alphabet_dict = defaultdict(list)

    for entry in entry_list:
        alphabet_dict[entry[0].upper()].append(entry)

    return alphabet_dict

def unique_publishers_report():
    """
    Get a report on the unique publishers in ``pub.db``

    Returns
    -------
    report : str
        Formatted report on the the unique publishers

    """

    pub = PubDB(read_only=True).pub

    unique_publishers = pub.get_unique_publishers()

    grouped_publishers = group_by_alphabet(unique_publishers)

    report = ''

    for letter, publishers in grouped_publishers.items():

        report += letter + '\n'
        report += '–' * 70 + '\n'

        for publisher in publishers:
            report += publisher + '\n'

        report += '\n'

    report = report.rstrip('\n')

    return report
