import pytest
import base64
import json
from unittest.mock import MagicMock
from sqlalchemy.exc import InvalidRequestError
from src.write_to_sql_v2 import write_to_database

@pytest.fixture
def mock_pubsub_event():
    """Fixture to simulate a Pub/Sub event."""
    mock_data = {
        'status': {
            'timestamp': '2024-10-07T08:07:44.594Z',
            'error_code': 0,
            'error_message': None,
            'elapsed': 11,
            'credit_count': 1,
            'notice': None,
            'total_count': 9838
        },
        'data': [
            {
                'id': 1,
                'name': 'Bitcoin',
                'symbol': 'BTC',
                'slug': 'bitcoin',
                'num_market_pairs': 11756,
                'date_added': '2010-07-13T00:00:00.000Z',
                'tags': [
                    # Your tags here...
                ],
                'max_supply': 21000000,
                'circulating_supply': 19764250,
                'total_supply': 19764250,
                'infinite_supply': False,
                'platform': None,
                'cmc_rank': 1,
                'self_reported_circulating_supply': None,
                'self_reported_market_cap': None,
                'tvl_ratio': None,
                'last_updated': '2024-10-07T08:05:00.000Z',
                'quote': {
                    'USD': {
                        'price': 63611.48092416422,
                        'volume_24h': 20426076858.256153,
                        'volume_change_24h': 66.9719,
                        'percent_change_1h': -0.10900847,
                        'percent_change_24h': 2.69951814,
                        'percent_change_7d': -1.38003038,
                        'percent_change_30d': 17.24061887,
                        'percent_change_60d': 11.10127252,
                        'percent_change_90d': 11.38559831,
                        'market_cap': 1257233211855.4126,
                        'market_cap_dominance': 56.862,
                        'fully_diluted_market_cap': 1335841099407.45,
                        'tvl': None,
                        'last_updated': '2024-10-07T08:05:00.000Z'
                    }
                }
            }
        ]
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
    mock_connector = mocker.patch('src.write_to_sql_v2.Connector')
    
    # Mock SQLAlchemy engine and connection
    mock_create_engine = mocker.patch('src.write_to_sql_v2.create_engine')
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    
    # Mock the server version to prevent TypeError
    mock_connection.scalar.return_value = '8.0.26'  # Mock MySQL server version

    # Call the function with the mocked Pub/Sub event
    write_to_database(mock_pubsub_event)

    # Assert that the engine was created and the connection was made
    mock_create_engine.assert_called_once()  # Ensure the engine was created
    mock_connection.execute.assert_called()  # Ensure the SQL statement was executed

    # Check if data was inserted successfully by confirming SQL queries were executed
    assert mock_connection.execute.call_count > 0

    print("Test passed!")
