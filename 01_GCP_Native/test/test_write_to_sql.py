import pytest
import base64
import json
from unittest.mock import MagicMock
import os

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Fixture to mock environment variables."""
    monkeypatch.setenv("PROJECT_ID", "test-project")
    monkeypatch.setenv("REGION", "us-central1")
    monkeypatch.setenv("INSTANCE_NAME", "test-instance")
    monkeypatch.setenv("DB_USER", "test-user")
    monkeypatch.setenv("DB_PASS", "test-pass")
    monkeypatch.setenv("DB_NAME", "test-db")

@pytest.fixture
def mock_pubsub_event():
    """Fixture to simulate a Pub/Sub event with data from assets/testing_cryptos.json."""
    
    # Load the JSON file data from the assets folder
    with open("./test/assets/testing_cryptos.json") as json_file:
        mock_data = json.load(json_file)
    
    # Encode the data as Pub/Sub would
    encoded_message = base64.b64encode(json.dumps(mock_data).encode('utf-8')).decode('utf-8')

    # Mock the cloud_event to simulate the behavior of cloud_event.data["message"]["data"]
    mock_event = MagicMock()
    mock_event.data = {
        "message": {
            "data": encoded_message
        }
    }

    return mock_event


def test_write_to_database(mocker, mock_pubsub_event, mock_env_vars):
    """Test the write_to_database function with mocked Cloud SQL and Pub/Sub."""

    # Mock the Google Cloud SQL connector
    mock_connector = mocker.patch('src.write_to_sql.Connector')

    # Mock SQLAlchemy engine and connection
    mock_create_engine = mocker.patch('src.write_to_sql.create_engine')
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_connection

    # Mock the server version to prevent TypeError
    mock_connection.scalar.return_value = '8.0.26'  # Mock MySQL server version

    # Import the function after environment variables are set
    from src.write_to_sql import write_to_database

    # Call the function with the mocked Pub/Sub event
    write_to_database(mock_pubsub_event)

    # Assert that the engine was created and the connection was made
    mock_create_engine.assert_called_once()  # Ensure the engine was created
    mock_connection.execute.assert_called()  # Ensure the SQL statement was executed

    # Check if data was inserted successfully by confirming SQL queries were executed
    assert mock_connection.execute.call_count > 0

    print("Test passed!")
