"""Functions related to parsing BeautifulSoup objects"""

import bs4

def load_soup(filepath, parser='xml'):
    """
    Load file to a ``BeautifulSoup`` object

    Parameters
    ----------
    filepath : str
        Path to a file
    parser : str
        Parser used by ``BeautifulSoup``
            Available parsers: ('html.parser', 'lxml', 'xml')

    Returns
    -------
    soup : bs4.BeautifulSoup | None
        Text loaded into a BeautifulSoup object; None if loading failed
    exception : None | type
        None if loading passed, an exception if it did not

    """

    soup, exception = None, None

    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            soup = bs4.BeautifulSoup(f.read(), features=parser)

    except (FileNotFoundError, PermissionError,
        IOError, UnicodeDecodeError) as exc:

        exception = exc

    return soup, exception

def convert_2_soup(input_data, parser='xml'):
    """
    Load file to a ``BeautifulSoup`` object or pass if a soup was inputted

    This function is useful for parsing, so a file has to be read and
        converted to a ``BeautifulSoup`` only once

    Parameters
    ----------
    input_data : bs4.BeautifulSoup | str
        Data loaded into ``BeautifulSoup`` or a path to a file to be parsed
    parser : str
        Parser used by ``BeautifulSoup``
            Available parsers: ('html.parser', 'lxml', 'xml')

    Returns
    -------
    soup : bs4.BeautifulSoup | None
        Text loaded into a ``BeautifulSoup object``; None if loading failed
    exception : None | type
        None if loading passed, an exception if it did not

    """

    soup, exception = None, None

    if isinstance(input_data, bs4.BeautifulSoup):
        soup = input_data

    elif isinstance(input_data, str):
        soup, exception = load_soup(input_data, parser=parser)

    else:
        exception = TypeError('Invalid input data type passed to convert_2_soup.')

    return soup, exception

def get_meta_content(soup, attr_value, attr_key='name'):
    """
    Retrieves the content of meta tags in the ``soup``

    For function usage, see examples below.

    Function behavior:

        - if the ``soup`` is not a ``BeautifulSoup`` instance,
            raises TypeError

        - if the search fails, returns ``None``

        - if the search results in one match, the content is returned
            as a string

        - if the search results in multiple matches, a list of the contents
            is returned

    Example (1)
    -----------

        If the soup contains the tag:

            <meta content="Nature Publishing Group" name="dc.publisher"/>

        this function can be used to retrieve the content:

            publisher = get_meta_content(soup, attr_value='dc.publisher')
            print(publisher)
            # Nature publishing group

    Example (2) 
    -----------
    
        If the soup contains the tag:

            <meta property="og:site_name" content="Nature"/>

        this function can be used to retrieve the content:

            site_name = get_meta_content(soup,
                attr_key='property',
                attr_value='og:site_name')

            print(site_name)
            # Nature

    Example (3)
    -----------

        If the soup contains the tags:

            <meta content="Author 1" name="dc.creator"/>
            <meta content="Author 2" name="dc.creator"/>

        this function can be used to retrieve the contents:

            authors = get_meta_content(soup, attr_value='dc.creator')

            for author in authors:
                print(author)

            # Author 1
            # Author 2

    Parameters
    ----------
    soup : bs4.BeautifulSoup
        A ``BeautifulSoup`` instance
    attr_value : str
        The value of the attribute to search for
    attr_key : str
        The key of the attribute to search for (defaults to "name")

    Returns
    -------
    contents : None | str | list of str
        The contents of the meta tag with the given attribute
            None if no match in the soup,
            string if one match,
            list of strings if multiple matches

    """

    if not isinstance(soup, bs4.BeautifulSoup):
        raise TypeError('A BeautifulSoup instance must be passed as the "soup" attribute of get_meta_content')

    contents = None

    _metas = soup.find_all('meta', attrs={attr_key: attr_value})

    if _metas:

        try:

            contents = [_meta['content'] for _meta in _metas]

            if len(contents) == 1:
                contents = contents[0]

        except KeyError:
            pass

    return contents
