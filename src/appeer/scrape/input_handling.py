"""Parse the scrape input and handle the result"""

import sys
import json

from appeer.general import utils
from appeer.general import log


def parse_data_source(publications):
    """
    (1) Analyze whether the inputted ``publications`` are a Python list, 
        a path to a JSON file or a path to a plaintext file.

    (2) Convert the inputted data source to a Python list

    Parameters
    ----------
    publications : list | str
        A list of URLs or a filepath to a JSON or plaintext file
            containing the URLs

    Returns
    -------
    data_source : list
        The data source converted to a Python list
    data_source_type : str 
        One of the following: 'pylist', 'JSON', 'plaintext',
            'invalid file', 'invalid input type'
    plaintext_ex_message : str
        The exception message catched while trying to read
            a text file containing URLs
    json_ex_message : str
        The exception message catched while trying to read
            a JSON file containing URLs

    """

    plaintext_ex_message, json_ex_message = None, None

    if isinstance(publications, list):

        data_source = publications
        data_source_type = 'pylist'

    elif isinstance(publications, str):

        try:

            data_source = utils.txt2list(publications)
            data_source_type = 'plaintext'

        except ValueError:

            _p_extype, plaintext_ex_message, _p_extcbk = sys.exc_info()

            try:

                data_source = utils.json2list(publications)
                data_source_type = 'JSON'

            except json.decoder.JSONDecodeError:

                _j_extype, json_ex_message, _j_extcbk = sys.exc_info()

                data_source = None
                data_source_type = 'invalid file'

        except FileNotFoundError:

            _p_extype, plaintext_ex_message, _p_extcbk = sys.exc_info()
            _j_extype, json_ex_message, _j_extcbk = sys.exc_info()

            data_source = None
            data_source_type = 'invalid file'

    else:

        data_source = None
        data_source_type = 'invalid input type'

    return data_source, data_source_type, plaintext_ex_message, json_ex_message

def handle_input_reading(publications,
        data_source_type,
        plaintext_ex_message,
        json_ex_message):
    """
    Handle reading of the input
        depending on the results of ``parse_input_data``

    Parameters
    ----------
    publications : list | str
        A list of URLs or a filepath to a JSON or plaintext file
            containing URLs
    data_source_type : str 
        One of the following: 'pylist', 'JSON', 'plaintext',
            'invalid file', 'invalid input type'
     plaintext_ex_message : str
        The exception message catched while trying to read
            a text file containing URLs
    json_ex_message : str
        The exception message catched while trying to read
            a JSON file containing URLs

    Returns
    -------
    success : bool
        True if parsing the input data went OK, False if it failed
    report : str
        Report on why parsing succeeded/failed

    """

    report = ''
    log_dashes = log.get_log_dashes()
    short_log_dashes = log.get_short_log_dashes()

    report += f'{log.boxed_message("READING INPUT FILE", centered=True)}\n\n'

    report += 'Attempting to read the input data...\n'

    match data_source_type:

        case 'invalid input type' :

            success = False
            report += 'Error: Input file was not provided or the input data type is invalid. A path to a JSON/text file or a Python list must be provided.'

        case 'invalid file' :

            success = False

            report += 'Error: The input file could not be read.\n'
            report += log_dashes + '\n'

            report += 'While attempting to read the file as plaintext, the following error occurred:\n'
            report += plaintext_ex_message + '\n'
            report += short_log_dashes + '\n'

            report += 'While attempting to read the file as JSON, the following error occurred:\n'
            report += json_ex_message

        case 'pylist':

            success = True

            report += 'Successfully read data from the input Python list!\n'

        case 'plaintext' :

            success = True

            report += f'Successfully read data from {publications} !\n'

        case 'JSON' :

            success = True

            report += f'Successfully read data from {publications} !\n'

    return success, report
