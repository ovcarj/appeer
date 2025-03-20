"""Functions related to parsing dates

Nomenclature for the functions:
    
    d : day (0-31), with an optional preceeding zero
        and optional suffix (st|nd|rd|th)

    m : month (01-12) with an optional preceeding zero
    M : month name (Jan or January, Feb or February, ...)

    y : year (any four-digit number is valid)

The normalize_* functions transform the date to the ISO format:

    YYYY-MM-DD

    e.g.:

    1st Feb 1993 -> 1993-02-01
    9 dec 2000   -> 2000-12-09
    30 10 1800   -> 1800-10-30

"""

import re
import datetime

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

def _M_map():
    """
    Defines a map of month names to month number (01-12)

    Returns
    -------
    _month_names : dict
        Dict of long and short forms of month names and month numbers

    """

    month_map = {
            'January': '01',
            'February': '02',
            'March': '03',
            'April': '04',
            'May': '05',
            'June': '06',
            'July': '07',
            'August': '08',
            'September': '09',
            'October': '10',
            'November': '11',
            'December': '12',
            'Jan': '01',
            'Feb': '02',
            'Mar': '03',
            'Apr': '04',
            # May already defined
            'Jun': '06',
            'Jul': '07',
            'Aug': '08',
            'Sep': '09',
            'Oct': '10',
            'Nov': '11',
            'Dec': '12'
            }

    return month_map

def _M_regex():
    """
    A regex to match a long or short month name

    Returns
    -------
    M_regex : str
        Regex matching long or short month name

    """

    month_names = _M_map().keys()

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

def normalize_d_M_y(dMY_date):
    """
    Transforms "d M y" strings to standard format (YYYY-MM-DD)

    E.g.:

    1st Feb 2010 -> 2010-02-01
    25 Dec 1990  -> 1990-12-25

    Parameters
    ----------
    dMY_date : str
        Date in "d M y" format

    Returns
    -------
    normalized_dMy_date : str | None
        Date in standard format; returns None on failure

    """

    try:
        d, M, y = dMY_date.split(' ')

    except (ValueError, AttributeError):
        return None

    try:
        day = re.findall(r'\b\d+', d)[0]

    except IndexError:
        return None

    if len(day) == 1:
        day = '0' + day

    try:
        month = _M_map()[M.capitalize()]

    except KeyError:
        return None

    normalized_dMy_date = '-'.join([y, month, day])

    try:
        datetime.date.fromisoformat(normalized_dMy_date)

    except ValueError:
        return None

    return normalized_dMy_date
