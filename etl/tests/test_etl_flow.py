import pytest
import logging
from unittest.mock import mock_open, patch, Mock, MagicMock
from datetime import datetime, timezone
import psycopg2

from jinja2 import Template

from utils import (
    reader,
    parse_etl_configs,
    get_logger,
    render_sql_from_file,
    build_values,
    build_params,
    DATALAKE_SCHEMA, ETL_TIMESTAMP,
    matcher,
    build_statistics,
)

from etl_flow import (
    RunETl,
    DB_SCHEMA_SQL_FILENAME,
    CLEAR_DWH_SQL_FILE,
    DWH_SQL_FOLDER_PATH,
)

from db_connector import (
    ConnectorDB
)


############ Testing utils.py ############

def test_reader_success():
    # Mocking a file read operation
    mock_data = '{"key": "value"}'
    with patch("builtins.open", mock_open(read_data=mock_data)):
        result = reader("dummy.json")
        assert result == {"key": "value"}


def test_reader_file_not_found():
    with pytest.raises(FileNotFoundError):
        reader("nonexistent.json")


@patch('utils.reader')
def test_parse_etl_configs_default_path(mock_reader):
    mock_reader.return_value = {"config": ["value"]}
    assert list(parse_etl_configs()) == [("config", "value")]


@patch('utils.reader')
def test_parse_etl_configs_custom_path(mock_reader):
    custom_config = {"custom": ["config"]}
    mock_reader.return_value = custom_config
    assert list(parse_etl_configs(conf_path="custom.json")) == [("custom", "config")]


@patch('utils.reader')
def test_parse_etl_configs_fallback_to_default(mock_reader):
    mock_reader.side_effect = [FileNotFoundError, {"default": ["config"]}]
    assert list(parse_etl_configs(conf_path="nonexistent.json")) == [("default", "config")]


@patch('utils.reader')
def test_parse_etl_configs_fallback_to_dwh(mock_reader):
    mock_reader.side_effect = [FileNotFoundError, {"dwh": ["config"]}]
    assert list(parse_etl_configs(conf_path="nonexistent.json", stage="not_datalake")) == [("dwh", "config")]


def test_get_logger():
    logger = get_logger("TestLogger")
    assert logger.name == "TestLogger"
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_render_sql_from_file_without_params():
    # Mocking a file read operation
    mock_sql_content = "SELECT * FROM table;"
    with patch("builtins.open", mock_open(read_data=mock_sql_content)):
        result = render_sql_from_file(file_name="query.sql")
        assert result == "SELECT * FROM table;"


def test_render_sql_from_file_with_params():
    # Mocking a file read operation and Template rendering
    mock_sql_content = "SELECT * FROM {{ table_name }};"
    with patch("builtins.open", mock_open(read_data=mock_sql_content)):
        with patch("jinja2.Template", new=Template):
            result = render_sql_from_file(file_name="query.sql", params_dict={"table_name": "users"})
            assert result == "SELECT * FROM users;"


def test_render_sql_from_file_with_formatted_path():
    # Mocking a file read operation with formatted path
    mock_sql_content = "SELECT * FROM table WHERE id = {{ id }};"
    mock_sql_folder_path = "/path/to/sql/{file_name}"
    expected_file_path = "/path/to/sql/query.sql"

    with patch("builtins.open", mock_open(read_data=mock_sql_content)) as mock_file:
        with patch("jinja2.Template", new=Template):
            render_sql_from_file(file_name="query.sql", params_dict={"id": 1}, sql_folder_path=mock_sql_folder_path)
            mock_file.assert_called_once_with(expected_file_path, 'r')


def test_build_values_with_empty_set():
    assert build_values(set()) == ""


def test_build_values_with_single_tuple():
    values = {(1, 'string', 3.14)}
    assert build_values(values) == "(1, 'string', 3.14)"


def test_build_values_with_multiple_tuples():
    values = [(1, 'a'), (2, 'b')]
    assert build_values(values) == "(1, 'a'), (2, 'b')"


def test_build_values_with_datetime():
    dt = datetime(2023, 1, 1)
    values = {(1, dt)}
    assert build_values(values) == f"(1, '{dt.isoformat()}')"


def test_build_values_with_mixed_types():
    dt = datetime(2023, 1, 1)
    values = {(1, 'test', dt)}
    assert build_values(values) == f"(1, 'test', '{dt.isoformat()}')"


# Mock for build_values if needed
@patch('utils.build_values', return_value="mocked values")
def test_build_params_with_all_fields(mock_build_values):
    table_name = "test_table"
    values = {(1, 'data')}
    fields = ['field1', 'field2']
    delete_where_fields = ['field1']
    schema = "test_schema"
    etl_timestamp = datetime.now()

    expected_result = {
        "table_name": table_name,
        "schema": schema,
        "fields": "field1, field2",
        "delete_where": "test_table.field1 = test_schema_test_table_temp.field1",
        "values": "mocked values",
        "etl_timestamp": etl_timestamp
    }

    result = build_params(table_name, values, fields, delete_where_fields, schema, etl_timestamp)
    assert result == expected_result
    mock_build_values.assert_called_once_with(values)


def test_build_params_with_no_values():
    table_name = "test_table"
    fields = ['field1', 'field2']
    schema = 'test_schema'
    etl_timestamp = ETL_TIMESTAMP

    expected_result = {
        "table_name": table_name,
        "schema": schema,
        "fields": "field1, field2",
        "delete_where": "test_table.field1 = test_schema_test_table_temp.field1 AND test_table.field2 = test_schema_test_table_temp.field2",
        "values": None,
        "etl_timestamp": etl_timestamp
    }

    result = build_params(table_name, schema=schema, fields=fields)
    assert result == expected_result


def test_build_params_with_defaults():
    table_name = "test_table"

    expected_result = {
        "table_name": table_name,
        "schema": DATALAKE_SCHEMA,
        "fields": None,
        "delete_where": "",
        "values": None,
        "etl_timestamp": ETL_TIMESTAMP
    }

    result = build_params(table_name)
    assert result == expected_result


def test_matcher_with_txt_extension():
    url = "http://example.com/file.txt"
    expected_regex = r'\|\|([^|^]+)\^'
    assert matcher(url) == expected_regex


def test_matcher_with_other_extension():
    url = "http://example.com/file.pdf"
    assert matcher(url) is None


def mock_function(*args, **kwargs):
    # Simulate some operation
    return "result"


@patch('utils.time', side_effect=[100, 200])  # Mock time to return fixed start and end times
def test_build_statistics_decorator(mock_time):
    decorated_function = build_statistics("test_schema")(mock_function)

    db_conn_mock = Mock()
    db_conn_mock.run_sql = Mock()

    # Create a mock object to simulate the first argument of the decorated function
    mock_obj = Mock()
    mock_obj.db_conn = db_conn_mock

    result = decorated_function(mock_obj)

    # Check if the original function's result is unchanged
    assert result == "result"

#####################################################################################################################

############ Testing etl_flow.py ############

@pytest.fixture
def mock_db_connector():
    with patch('etl_flow.ConnectorDB') as mock:
        yield mock()


@pytest.fixture
def mock_requests_get():
    with patch('requests.get') as mock:
        yield mock


@pytest.fixture
def mock_logger():
    with patch('etl_flow.get_logger') as mock:
        yield mock()


def test_init(mock_db_connector):
    etl = RunETl()
    assert etl.db_conn is mock_db_connector


def test_etl_1_build_schema(mock_db_connector, mock_logger):
    etl = RunETl()
    etl.etl_1_build_schema()
    mock_db_connector.run_sql.assert_called_with(sql_file_name=DB_SCHEMA_SQL_FILENAME)


def test_populate_datalake_success(mock_db_connector, mock_requests_get, mock_logger):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = 'sample data\n'
    mock_requests_get.return_value = mock_response

    etl = RunETl()
    etl.populate_datalake(mock_response, 'ads_and_trackers', 'source')


def test_populate_datalake_failure(mock_db_connector, mock_requests_get, mock_logger):
    mock_response = Mock()
    mock_response.status_code = 404
    mock_requests_get.return_value = mock_response

    etl = RunETl()
    etl.populate_datalake(mock_response, 'ads_and_trackers', 'source')


def test_clear_dwh(mock_db_connector, mock_logger):
    etl = RunETl()
    etl.clear_dwh()
    mock_db_connector.run_sql.assert_called_with(sql_file_name=CLEAR_DWH_SQL_FILE, sql_folder_path=DWH_SQL_FOLDER_PATH)


def test_populate_dwh(mock_db_connector):
    etl = RunETl()
    etl.populate_dwh('table', 'schema')


# Test Full Data Flow
@patch('etl_flow.RunETl.etl_1_build_schema')
@patch('etl_flow.RunETl.etl_2_build_datalake')
@patch('etl_flow.RunETl.etl_3_build_dwh')
def test_data_flow(mock_etl_1, mock_etl_2, mock_etl_3, mock_logger):
    etl = RunETl()
    etl.data_flow()

    mock_etl_1.assert_called()
    mock_etl_2.assert_called()
    mock_etl_3.assert_called()

#####################################################################################################################

############ Testing db_connector.py ############


@pytest.fixture
def mock_psycopg2_connect():
    with patch('psycopg2.connect') as mock:
        yield mock


@pytest.fixture
def mock_os_getenv():
    with patch('os.getenv') as mock:
        mock.return_value = 'dummy_value'
        yield mock


def test_create_db_connection_success(mock_psycopg2_connect, mock_os_getenv, mock_logger):
    db = ConnectorDB(logger=mock_logger)
    connection = db.create_db_connection()
    assert mock_psycopg2_connect.called
    assert connection is not None


def test_create_db_connection_failure(mock_psycopg2_connect, mock_os_getenv, mock_logger):
    mock_psycopg2_connect.side_effect = psycopg2.OperationalError

    db = ConnectorDB(logger=mock_logger)
    with pytest.raises(psycopg2.OperationalError):
        db.create_db_connection()
    mock_logger.error.assert_called()


@patch('db_connector.render_sql_from_file', return_value="SELECT 1;")
def test_run_sql_success(mock_render_sql, mock_psycopg2_connect, mock_os_getenv, mock_logger):
    db = ConnectorDB(logger=mock_logger)
    row_count = db.run_sql(sql_file_name="dummy.sql")
    assert mock_psycopg2_connect.called
    assert row_count is not None
    mock_logger.info.assert_called()


@patch('db_connector.render_sql_from_file', return_value="SELECT 1;")
def test_run_sql_with_retries(mock_render_sql, mock_psycopg2_connect, mock_os_getenv, mock_logger):
    mock_psycopg2_connect.side_effect = [psycopg2.OperationalError, MagicMock()]

    db = ConnectorDB(logger=mock_logger)
    db.run_sql(sql_file_name="dummy.sql")
    assert mock_psycopg2_connect.call_count == 2
    mock_logger.error.assert_called()


#####################################################################################################################


if __name__ == '__main__':
    pytest.main()
