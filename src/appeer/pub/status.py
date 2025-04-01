"""Get reports on the current status of publisher/journal entries in pub.db"""

import textwrap

from collections import defaultdict

from appeer.general import utils as _utils
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
    Get an alphabetical list of unique publishers in ``pub.db``

    Returns
    -------
    report : str
        Alphabetical list of unique publishers

    """

    pub = PubDB(read_only=True).pub

    unique_publishers = pub.get_unique_publishers()

    if not unique_publishers:
        return 'No publishers found.'

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

def unique_journals_report(publisher):
    """
    Get an alphabetical list of journals for a given ``publisher``

    Parameters
    ----------
    publisher : str
        Normalized publisher name

    Returns
    -------
    report : str
        Alphabetical list of journals for a given publisher

    """

    pub = PubDB(read_only=True).pub

    unique_journals = pub.get_unique_journals(publisher)

    if not unique_journals:
        return f'No journals found for publisher "{publisher}".'

    grouped_journals = group_by_alphabet(unique_journals)

    report = ''

    for letter, journals in grouped_journals.items():

        report += letter + '\n'
        report += '–' * 70 + '\n'

        for journal in journals:
            report += journal + '\n'

        report += '\n'

    report = report.rstrip('\n')

    return report

def publisher_summary_report(publisher):
    """
    A semi-detailed summary of the journals found for a given ``publisher``

    Parameters
    ----------
    publisher : str
        Normalized publisher name

    Returns
    -------
    report : str
        Summary of the journals found for a given ``publisher``

    """

    pub = PubDB(read_only=True).pub

    publisher_summary = pub.get_publisher_summary(publisher=publisher)

    if not publisher_summary:
        return f'No entries found for publisher "{publisher}".'

    report = _log.boxed_message(f'Summary of publisher: {publisher}', centered=True) + '\n\n'

    align = len(max([
        'journal_name',
        'no_of_entries',
        'min_received',
        'max_received',
        'min_accepted',
        'max_accepted',
        'min_published',
        'max_published',
        'publication_types'], key=len)) + 2

    for journal_summary in publisher_summary:

        report += _journal_summary_msg(
                journal_summary=journal_summary,
                align=align)

        report += '\n\n'

    report = report.rstrip()

    return report

def _journal_summary_msg(journal_summary, align=None):
    """
    Create a boxed summary from appeer.db.tables.pub.JournalSummary

    Parameters
    ----------
    journal_summary : appeer.db.tables.pub.JournalSummary
        Journal summary
    align : None | int
        Optional alignment for printing; if None, it will be calculated

    Returns
    -------
    msg : str
        Boxed journal summary

    """

    if not align:

        align = len(max([
            'journal_name',
            'no_of_entries',
            'min_received',
            'max_received',
            'min_accepted',
            'max_accepted',
            'min_published',
            'max_published',
            'publication_types'], key=len)) + 2

    publication_types = ", ".join(
            _utils.publication_types_unpack(
                publication_types_pack=journal_summary.publication_types)
            )

    start_ = '\n' + ' ' * (align + 1)

    publication_types = start_.join(textwrap.wrap(publication_types,
            width=len(journal_summary.name),
            break_long_words=False))

    _msg = f'{"no_of_entries":<{align}} {journal_summary.count}\n\n'
    _msg += f'{"min_received":<{align}} {journal_summary.min_received}\n'
    _msg += f'{"max_received":<{align}} {journal_summary.max_received}\n\n'
    _msg += f'{"min_accepted":<{align}} {journal_summary.min_accepted}\n'
    _msg += f'{"max_accepted":<{align}} {journal_summary.max_accepted}\n\n'
    _msg += f'{"min_published":<{align}} {journal_summary.min_published}\n'
    _msg += f'{"max_published":<{align}} {journal_summary.max_published}\n\n'
    _msg += f'{"publication_types":<{align}} {publication_types}'

    msg = _log.boxed_message(_msg, header=journal_summary.name, centered=True)

    return msg
