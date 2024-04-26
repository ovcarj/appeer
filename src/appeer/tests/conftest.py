import pytest
import pathlib

@pytest.fixture
def sample_data_path(scope='session'):
    """
    Get full path to ``./src/appeer/tests/sample_data``.
    """

    path_to_sample_data = pathlib.Path('./src/appeer/tests/sample_data')

    return path_to_sample_data

@pytest.fixture
def sample_json_path(sample_data_path, scope='session'):
    """
    Get full path to ``PoP.json`` located in ``sample_data``.
    """

    path_to_sample_json = pathlib.Path(f'{sample_data_path}/PoP.json')

    return path_to_sample_json
