import pytest
import base64
import json
from unittest.mock import MagicMock
from sqlalchemy.exc import InvalidRequestError  # Import this to handle SQLAlchemy errors
from src.write_to_sql import write_to_database  # Import your function

@pytest.fixture
def mock_pubsub_event():
    """Fixture to simulate a Pub/Sub event."""
    mock_data = {
        "uuid": "Qwsogvtv82FCd",
        "symbol": "BTC",
        "name": "Bitcoin",
        "color": "#f7931A",
        "iconUrl": "https://cdn.coinranking.com/bOabBYkcX/bitcoin_btc.svg",
        "marketCap": "1188473660704",
        "price": "60138.02048391904",
        "listedAt": 1330214400,
        "tier": 1,
        "change": "-2.57",
        "rank": 1,
        "sparkline": ["61633.59175918901", "61191.99887071534"],
        "lowVolume": False,
        "coinrankingUrl": "https://coinranking.com/coin/Qwsogvtv82FCd+bitcoin-btc",
        # "24hVolume": "47603655385",
        "volume_24h": "47603655385",
        "btcPrice": "1",
        "contractAddresses": []
    }

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


def test_write_to_database(mocker, mock_pubsub_event):
    """Test the write_to_database function with mocked Cloud SQL and Pub/Sub."""
    
    # Mock the Google Cloud SQL connector
    mock_connector = mocker.patch('src.write_to_sql.Connector')  # Updated import for Connector
    
    # Mock SQLAlchemy engine and connection
    mock_create_engine = mocker.patch('src.write_to_sql.sqlalchemy.create_engine')  # Updated import
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    
    # Call the function with the mocked Pub/Sub event
    write_to_database(mock_pubsub_event)

    # Assert that the engine was created and the connection was made
    mock_create_engine.assert_called_once()  # Ensure the engine was created
    mock_connection.execute.assert_called()  # Ensure the SQL statement was executed

    # Check if data was inserted successfully by confirming SQL queries were executed
    assert mock_connection.execute.call_count > 0

    print("Test passed!")
    
    
def test_write_to_database_missing_24hVolume(mocker, mock_pubsub_event):
    """Test the write_to_database function with missing '24hVolume' parameter."""
    
    # Mock the Google Cloud SQL connector
    mock_connector = mocker.patch('src.write_to_sql.Connector')
    
    # Mock SQLAlchemy engine and connection
    mock_create_engine = mocker.patch('src.write_to_sql.sqlalchemy.create_engine')
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    
    # Remove '24hVolume' key to simulate the error
    del mock_pubsub_event.data['message']['data']['24hVolume']  # Simulate missing volume_24h

    # Simulate raising SQLAlchemy InvalidRequestError
    with pytest.raises(sqlalchemy.exc.InvalidRequestError) as exc_info:
        write_to_database(mock_pubsub_event)

    # Assert that the error message matches the expected SQLAlchemy error
    assert "A value is required for bind parameter '24hVolume'" in str(exc_info.value)

    print("Test passed with missing '24hVolume'!")
    

def test_write_to_database_with_volume_24h_key(mocker, mock_pubsub_event):
    """Test to trigger an error due to missing '24hVolume' in the dictionary."""
    
    # Mock the Google Cloud SQL connector
    mock_connector = mocker.patch('src.write_to_sql.Connector')
    
    # Mock SQLAlchemy engine and connection
    mock_create_engine = mocker.patch('src.write_to_sql.sqlalchemy.create_engine')
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    
    # Call the function with the mocked Pub/Sub event
    # Expecting the error due to missing '24hVolume'
    with pytest.raises(InvalidRequestError) as exc_info:
        write_to_database(mock_pubsub_event)

    # Assert that the error is related to the missing '24hVolume' bind parameter
    assert "A value is required for bind parameter '24hVolume'" in str(exc_info.value)

    print("Test passed with missing '24hVolume'!")
