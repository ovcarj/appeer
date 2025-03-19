"""Default list of metadata properties to be parsed from a publication"""

from appeer.db.tables.registered_tables import get_registered_tables

def default_metadata():
    """
    Default list of metadata properties to be parsed from a publication

    The list is obtained by reading the registered columns of the
        "parses" table and removing the ParseAction related columns

    Returns
    -------
    metadata_list : list of str
        Default metadata properties

    """

    parses_table_columns = get_registered_tables()['parses']

    metadata_list = [column for column in parses_table_columns
            if column not in (
                'label',
                'action_index',
                'scrape_label',
                'scrape_action_index',
                'date',
                'input_file',
                'parser',
                'success',
                'status',
                'committed'
                )
            ]

    return metadata_list
