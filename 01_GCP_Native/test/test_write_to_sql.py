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
        'uuid': 'Qwsogvtv82FCd',
        'symbol': 'BTC',
        'name': 'Bitcoin',
        'color': '#f7931A',
        'iconUrl': 'https://cdn.coinranking.com/bOabBYkcX/bitcoin_btc.svg',
        'marketCap': '1208721081869',
        'price': '61161.7009785164',
        'listedAt': 1330214400,
        'tier': 1,
        'change': '-0.03',
        'rank': 1,
        'sparkline': ['61221.27285422067', '61290.247255518', '61224.68319446232', '60500.04412073567', '60549.690608442324', '60815.006722737206', '60871.06438227951', '60560.21107766191', '60350.11137908651', '60508.70660097368', None, '60320.28266676264', '60363.15946319355', '60557.01348093903', '60893.56516232311', '60876.218517752604', '60811.587762462', '60853.71912132096', '60814.56146480672', '60673.67212747346', '60897.01469655926', '61111.24942717578', '60968.03724828914', '61064.369313167655'],
        'lowVolume': False,
        'coinrankingUrl': 'https://coinranking.com/coin/Qwsogvtv82FCd+bitcoin-btc',
        '24hVolume': '38484411460',
        'btcPrice': '1',
        'contractAddresses': []
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
