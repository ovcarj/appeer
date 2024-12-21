"""#TODO Information on scientific journals"""

from collections import namedtuple

Journal = namedtuple('Journal', [
    'web_domain',           # Unique web domain
    'publisher_code'        # Internal publisher code
    'publisher',            # Full name of the journal publisher
    'journal_code',         # Internal journal code
    'journal',              # Full name of the journal
    'scientific_scope',     # Arbitrary list of scientific keywords
    'description'           # Arbitrary journal description
    ])
