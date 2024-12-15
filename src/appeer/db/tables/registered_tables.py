"""Defines the allowed table names and columns for SQL query sanitization"""

def get_registered_tables():
    """
    Defines the allowed table names and columns for SQL query sanitization

    Returns
    -------
    registered_tables : dict
        Dictionary of allowed table names and the corresponding columns

    """

    registered_tables = {

            'scrape_jobs':
                ['label',
                'description',
                'log',
                'download_directory',
                'zip_file',
                'date',
                'job_status',
                'job_step',
                'job_successes',
                'job_fails',
                'no_of_publications',
                'job_parsed'],

            'scrapes':
                ['label',
                 'scrape_index',
                 'url',
                 'strategy',
                 'status',
                 'out_file',
                 'parsed'],

            'parse_jobs':
                ['label',
                 'description',
                 'log',
                 'mode',
                 'parse_directory',
                 'date',
                 'job_status',
                 'job_successes',
                 'job_fails',
                 'no_of_publications',
                 'job_committed'],

            'pubs':
                ['doi',
                 'received',
                 'accepted',
                 'published',
                 'duration',
                 'publisher',
                 'journal',
                 'title',
                 'affiliations']
 
                }

    return registered_tables

def sanity_check(name, columns):
    """
    Checks if ``name`` and ``columns`` are in the registered tables

    Parameters
    ----------
    name : str
        Table name
    columns : list
        List of column names

    """

    registered_tables = get_registered_tables()

    allowed_names = registered_tables.keys()

    if name not in allowed_names:
        raise PermissionError(f'Table name not in registered tables. Allowed names: {list(allowed_names)}')

    allowed_columns = registered_tables[name]

    if columns != allowed_columns:
        raise PermissionError(f'Incorrect table columns found. Expected columns: {allowed_columns}')

def check_column(name, column):
    """
    Checks if ``column`` is in the registered table called ``name``

    """

    registered_tables = get_registered_tables()

    allowed_names = registered_tables.keys()

    if name not in allowed_names:
        raise PermissionError(f'Table name not in registered tables. Allowed names: {list(allowed_names)}')

    allowed_columns = registered_tables[name]

    if not column in allowed_columns:
        raise PermissionError(f'Incorrect table column {column} given. Expected one of the following columns: {allowed_columns}')
