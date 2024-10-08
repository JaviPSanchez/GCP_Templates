print("Loading Libraries....")
import os
import logging
import base64
import json
from datetime import datetime
from dotenv import load_dotenv
# ORM
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
# Google Cloud Services
import functions_framework
from google.cloud.sql.connector import Connector
print("Done!!!")

# Load environment variables from .env file
load_dotenv(".env.local")

# Google Cloud SQL connection information
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
INSTANCE_NAME = os.getenv("INSTANCE_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Instance connection name to connect to DB
INSTANCE_CONNECTION_NAME = f"{PROJECT_ID}:{REGION}:{INSTANCE_NAME}"

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def write_to_database(cloud_event):
    """Cloud_event object, which is supposed to be passed in by the
    Google Cloud Functions framework when an event is triggered.

    Args:
        cloud_event (Object):
    """
    print(f"Cloud Event: {cloud_event}")

    def ingest_to_db(str_record):
        print("Ingest to DB started")

        # Initialize the Connector for Google Cloud SQL
        connector = Connector()

        try:
            # Load the JSON string
            print("------------------------------")
            print(f"Raw string before JSON load: {str_record}")
            print("------------------------------")
            # Replace single quotes with double quotes to make it valid JSON format
            # str_record = str_record.replace("'", '"')
            # Replace Python None with JSON null
            # str_record = str_record.replace('None', 'null')
            # Replace Python True/False with JSON true/false
            # str_record = str_record.replace('True', 'true')
            # str_record = str_record.replace('False', 'false')
            dict_record = json.loads(str_record)
            print("------------------------------")
            print(f"JSON loaded!: {dict_record}")
            print("------------------------------")
            
            print(f"Preparing Json....")
            
            status = dict_record["status"]["timestamp"]
            status = datetime.strptime(status, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
            print(f"Status: {status}")
            data = dict_record["data"][0]
            print(f"Data: {data}")
            
            # Convert 'date_added' and 'last_updated' to MySQL datetime format
            if data.get('date_added'):
                data['date_added'] = datetime.strptime(data['date_added'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
        
            if data.get('last_updated'):
                data['last_updated'] = datetime.strptime(data['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
            
            # Prepare the data (modify dict_record keys to match the columns)
            dict_record = {
                'status': status,
                'id': data['id'],
                'name': data['name'],
                'symbol': data['symbol'],
                'slug': data['slug'],
                'num_market_pairs': data['num_market_pairs'],
                'date_added': data['date_added'],
                'tags': json.dumps(data['tags']),
                'max_supply': data['max_supply'],
                'circulating_supply': data['circulating_supply'],
                'total_supply': data['total_supply'],
                'infinite_supply': data['infinite_supply'],
                'platform': data.get('platform'),
                'cmc_rank': data['cmc_rank'],
                'self_reported_circulating_supply': data['self_reported_circulating_supply'],
                'self_reported_market_cap': data['self_reported_market_cap'],
                'tvl_ratio': data['tvl_ratio'],
                'last_updated': data['last_updated'],
                'price': data['quote']['USD']['price'],
                'volume_24h': data['quote']['USD']['volume_24h'],
                'volume_change_24h': data['quote']['USD']['volume_change_24h'],
                'percent_change_1h': data['quote']['USD']['percent_change_1h'],
                'percent_change_24h': data['quote']['USD']['percent_change_24h'],
                'percent_change_7d': data['quote']['USD']['percent_change_7d'],
                'percent_change_30d': data['quote']['USD']['percent_change_30d'],
                'percent_change_60d': data['quote']['USD']['percent_change_60d'],
                'percent_change_90d': data['quote']['USD']['percent_change_90d'],
                'market_cap': data['quote']['USD']['market_cap'],
                'market_cap_dominance': data['quote']['USD']['market_cap_dominance'],
                'fully_diluted_market_cap': data['quote']['USD']['fully_diluted_market_cap'],
                'tvl': data['quote']['USD']['tvl']
            }
            
            print(f"Done!!!")
            print("----------------------------------------------------")
            print(f"Json: {dict_record}")

            # Function to get a connection to the Cloud SQL instance
            def getconn():
                conn = connector.connect(
                    INSTANCE_CONNECTION_NAME,
                    "pymysql",
                    user=DB_USER,
                    password=DB_PASS,
                    db=DB_NAME,
                )
                print(f"Connecting to instance: {INSTANCE_CONNECTION_NAME}")
                return conn

            # Create a connection pool with SQLAlchemy
            pool = create_engine(
                "mysql+pymysql://",
                creator=getconn,
            )
            print("Connection pool created")


            # Connect to the connection pool and execute queries
            with pool.connect() as db_conn:
                print("Connected to the database")

                # Insert data into the coin_cryptos table
                insert_statement = """
                INSERT INTO coin_cryptos (
                    status, id, name, symbol, slug, num_market_pairs, date_added, tags, max_supply,
                    circulating_supply, total_supply, infinite_supply, platform, cmc_rank,
                    self_reported_circulating_supply, self_reported_market_cap, tvl_ratio, last_updated,
                    price, volume_24h, volume_change_24h, percent_change_1h, percent_change_24h,
                    percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d,
                    market_cap, market_cap_dominance, fully_diluted_market_cap, tvl
                ) VALUES (
                    :status, :id, :name, :symbol, :slug, :num_market_pairs, :date_added, :tags, :max_supply,
                    :circulating_supply, :total_supply, :infinite_supply, :platform, :cmc_rank,
                    :self_reported_circulating_supply, :self_reported_market_cap, :tvl_ratio, :last_updated,
                    :price, :volume_24h, :volume_change_24h, :percent_change_1h, :percent_change_24h,
                    :percent_change_7d, :percent_change_30d, :percent_change_60d, :percent_change_90d,
                    :market_cap, :market_cap_dominance, :fully_diluted_market_cap, :tvl
                )
                """
                
                db_conn.execute(text(insert_statement), dict_record)
                
                # Commit the transaction to ensure changes are saved in DB
                db_conn.commit()

                print("Data inserted successfully!")
                
        except SQLAlchemyError as e:
            logging.exception(f"SQLAlchemy Error: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Error loading JSON: {e}")
            logging.error(f"Invalid JSON string: {str_record}")
        except Exception as e:
            logging.exception(f"Unexpected error: {e}")
    
    # Get the message from the event that triggered this run
    t_record = base64.b64decode(cloud_event.data["message"]["data"])
    print(f"Raw Pub/Sub message: {cloud_event.data['message']['data']}")

    # Make sure that it's formatted as a UTF-8 string
    str_record = str(t_record, 'utf-8')
    print(f"Formatted message: {str_record}")

    # Start the ingestion process
    ingest_to_db(str_record)
    print("Ingest to DB function completed")

