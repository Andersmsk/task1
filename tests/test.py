import pytest
from unittest.mock import MagicMock
from main import DatabaseConnection, JSONFile, XMLFile, main

@pytest.fixture
def mock_database_connection():
    config = {
        "DB_USERNAME": "testuser",
        "DB_PASSWORD": "testpassword",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_DATABASE": "testdb"
    }
    database = DatabaseConnection(config)
    database.connect()
    yield database
    database.close()

def test_database_connection_connect(mock_database_connection):
    assert mock_database_connection.connection is not None
    assert mock_database_connection.cursor is not None

def test_json_file_read_file(tmp_path):
    json_data = {"key": "value"}
    json_file = tmp_path / "test.json"
    json_file.write_text(json.dumps(json_data))

    data = JSONFile.read_file(json_file)

    assert data == json_data

def test_xml_file_save_file(tmp_path):
    data = {
        "columns": ["column1", "column2"],
        "rows": [
            {"column1": "value1", "column2": "value2"},
            {"column1": "value3", "column2": "value4"}
        ]
    }
    xml_file = tmp_path / "test.xml"

    XMLFile.save_file(data, xml_file)

    assert xml_file.exists()

def test_main_execution(mock_database_connection, tmp_path):
    students_file = tmp_path / "students.json"
    rooms_file = tmp_path / "rooms.json"
    output_format = "json"

    # Mock JSONFile.read_file to return test data
    JSONFile.read_file = MagicMock(return_value={"key": "value"})

    main(students_file, rooms_file, output_format)

    # Add your assertions here to check the expected behavior of the main function

if __name__ == "__main__":
    pytest.main()