"""Functions related to parsing dates

Nomenclature for the functions:
    
    d : day (0-31), with an optional preceeding zero
        and optional suffix (st|nd|rd|th)

    m : month (01-12) with an optional preceeding zero
    M : month name (Jan or January, Feb or February, ...)

    y : year (any four-digit number is valid)

"""

import re

def _d_regex():
    """
    A regex matching (01-31) with an optional preceeding 0 and optional suffix

    Returns
    -------
    d_regex : str
        A regex matching (01-31) with an optional preceeding 0
            and suffix (st|nd|rd|th)

    """

    d_regex = r'\b(0?[1-9]|1[0-9]|2[0-9]|3[0-1])(st|nd|rd|th)?\b'

    return d_regex

def _M_list():
    """
    Defines a list of month names, including long and short form

    Returns
    -------
    _month_names : list of str
        List of long and short forms of month names

    """

    month_names = [
            'January',
            'February',
            'March',
            'April',
            'May',
            'June',
            'July',
            'August',
            'September',
            'October',
            'November',
            'December',
            'Jan',
            'Feb',
            'Mar',
            'Apr',
            'May',
            'Jun',
            'Jul',
            'Aug',
            'Sep',
            'Oct',
            'Nov',
            'Dec'
            ]

    return month_names

def _M_regex():
    """
    A regex to match a long or short month name

    Returns
    -------
    M_regex : str
        Regex matching long or short month name

    """

    month_names = _M_list()

    M_regex = r'\b(' + r'|'.join(month_names) + r')\b'

    return M_regex

def _y_regex():
    """
    A regex to match a year (four-digit number)

    Returns
    -------
    y_regex : str
        Regex matching a year (four-digit number)

    """

    y_regex = r'\b[0-9]{4}\b'

    return y_regex

def get_d_M_y(entry):
    """
    Matches "d M y" substrings; see module documentation for nomenclature

    The matches are returned in a list of strings.

    The case in month name is ignored.

    Examples that are matched:
        
        18th October 2023
        05 Jun 2060
        31 February 9999
        1st dec 0000

    Parameters
    ----------
    entry : str
        String in which the match is searched for

    Returns
    -------
    _d_M_y : list of str | None
        The matched substring; None if no matches were found

    """

    if not isinstance(entry, str):
        return None

    d = _d_regex()
    M = _M_regex()
    y = _y_regex()

    matches = re.finditer(rf'{d} {M} {y}', entry, re.IGNORECASE)

    result = [match.group() for match in matches] or None

    return result
