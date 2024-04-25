import appeer.utils
import pathlib
import pytest

import time
import datetime

@pytest.fixture
def sample_data_path():
    """
    Get full path to ``$APPEER_ROOT_DIR/src/appeer/tests/sample_data``.
    """

    path_to_sample_data = pathlib.Path('./src/appeer/tests/sample_data')

    return path_to_sample_data

@pytest.fixture
def sample_json_path(sample_data_path):
    """
    Get full path to ``PoP.json`` located in ``sample_data``.
    """

    path_to_sample_json = pathlib.Path(f'{sample_data_path}/PoP.json')

    return path_to_sample_json

def test_load_json(sample_json_path):
    """
    Test if the example JSON is loaded to a nonempty list of nonempty dictionaries.
    """

    sample_json = sample_json_path
    loaded_json = appeer.utils.load_json(sample_json)

    assert type(loaded_json) == list
    assert loaded_json is not []

    for json_element in loaded_json:
        assert type(json_element) == dict
        assert json_element is not {}

def test_get_current_datetime():
    """
    Test if the current datetime string has the correct format.
    """

    timestr = appeer.utils.get_current_datetime()

    assert type(timestr) == str

    year = int(timestr[0:4])
    month = int(timestr[4:6])
    day = int(timestr[6:8])

    assert timestr[8] == '-'

    hour = int(timestr[9:11])
    minute = int(timestr[11:13])
    second = int(timestr[13:15])

    assert year >= 2024
    assert 1 <= month <= 12
    assert 1 <= day <= 31
    assert 0 <= hour <= 24
    assert 0 <= minute <= 60
    assert 0 <= second <= 60

def test_convert_time_string():
    """
    Test if conversion from the ``%Y%m%d-%H%M%S`` format to ``datetime.datetime`` object works.
    """

    example_date = '20240812-090801'

    datetime_object = appeer.utils.convert_time_string(example_date)

    assert type(datetime_object) == datetime.datetime

    convert_back = datetime_object.strftime('%Y%m%d-%H%M%S')

    assert convert_back == example_date

def test_get_runtime():
    """
    Test if runtime is reasonably calculated.
    """

    start_datetime = appeer.utils.get_current_datetime()
    time.sleep(0.5)
    end_datetime = appeer.utils.get_current_datetime()

    runtime = appeer.utils.get_runtime(appeer.utils.convert_time_string(start_datetime),
            appeer.utils.convert_time_string(end_datetime))

    assert type(runtime) == str

    datetime_object = datetime.datetime.strptime(runtime, "%H:%M:%S")
    assert type(datetime_object) == datetime.datetime

    seconds = int(runtime[-2:])
    assert seconds < 2

def test_write_text_to_file(tmp_path):
    """
    Test if text data is correctly written to a file.
    """

    text = 'Hello from appeer'

    tmp_dir = tmp_path / 'test_write_text'
    tmp_dir.mkdir()

    filepath = tmp_dir / 'hello.txt'

    appeer.utils.write_text_to_file(path_to_file=filepath, text_data=text)

    assert filepath.read_text() == text
    assert len(list(tmp_path.iterdir())) == 1

def test_archive_directory(tmp_path):
    """
    Test if creating a .zip file from a directory is successfull.
    """

    tmp_dir = tmp_path / 'test_archive'
    tmp_dir.mkdir()
    filepath = tmp_dir / 'sample.txt'
    appeer.utils.write_text_to_file(path_to_file=filepath, text_data='.zip test')

    zip_filename = str(tmp_dir / 'test.zip')

    appeer.utils.archive_directory(output_filename=zip_filename, directory_name=tmp_dir)
    assert pathlib.Path(zip_filename).is_file()

    # TODO: unpack archive and check content

def test_delete_directory(tmp_path):
    """
    Test if deleting a directory works.
    """

    tmp_dir = tmp_path / 'test_delete'
    tmp_dir.mkdir()

    assert len(list(tmp_path.iterdir())) == 1

    appeer.utils.delete_directory(tmp_dir)

    assert len(list(tmp_path.iterdir())) == 0
