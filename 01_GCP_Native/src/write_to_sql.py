print("Loading Libraries....")
import os
import logging
import base64
import sqlalchemy
import json
import re
from datetime import datetime
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import functions_framework
from collections import OrderedDict
print("Done!!!")

# Load environment variables from .env file
load_dotenv(".env.local")

# Set the environment variable for Google Application Credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key_access_sql.json"

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # You can change this to DEBUG for more detailed logs

# Google Cloud SQL connection information
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
INSTANCE_NAME = os.getenv("INSTANCE_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Instance connection name
INSTANCE_CONNECTION_NAME = f"{PROJECT_ID}:{REGION}:{INSTANCE_NAME}"
# print(f"Instance connection name: {INSTANCE_CONNECTION_NAME}")


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def write_to_database(cloud_event):
    """Cloud_event object, which is supposed to be passed in by the
    Google Cloud Functions framework when an event is triggered.

    Args:
        cloud_event (Object): 
    """

    def ingest_to_db(str_record):
        print("Ingest to DB started")

        # Initialize the Connector for Google Cloud SQL
        connector = Connector()
        db_conn = None

        # Fix the record format if necessary
        def fix_single_quotes(str_record):
            # Replace Python-style booleans and None with their JSON equivalents
            str_record = str_record.replace("None", "null")
            str_record = str_record.replace("True", "true")
            str_record = str_record.replace("False", "false")
            # Use regex to replace single quotes with double quotes for JSON properties and values
            str_record = re.sub(r"'", '"', str_record)
            return str_record

        # Fix the record format
        str_record = fix_single_quotes(str_record)
        print(f"Fixed record format: {str_record}")

        # Load the JSON string properly
        try:
            print(f"Starting JSON load.....")
            dict_record = json.loads(str_record)
            dict_record['created_at'] = datetime.now()
            print(f"Finished JSON treatment")
        except json.JSONDecodeError as e:
            logging.error(f"Error loading JSON: {e}")
            logging.error(f"Invalid JSON string: {str_record}")
            return  # Exit the function early if JSON is invalid
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

        # Rename '24hVolume' key to 'Volume24h' to match the database column
        if '24hVolume' in dict_record:
            dict_record['Volume24h'] = dict_record.pop('24hVolume')

        # Convert string numbers to appropriate numeric types
        dict_record['marketCap'] = int(dict_record['marketCap']) if dict_record.get('marketCap') else None
        dict_record['price'] = float(dict_record['price']) if dict_record.get('price') else None
        dict_record['btcPrice'] = float(dict_record['btcPrice']) if dict_record.get('btcPrice') else None
        dict_record['Volume24h'] = int(dict_record['Volume24h']) if dict_record.get('Volume24h') else None
        dict_record['change'] = float(dict_record['change']) if dict_record.get('change') else None
        dict_record['lowVolume'] = bool(dict_record['lowVolume'])


        # Convert lists to JSON strings
        dict_record['sparkline'] = json.dumps(dict_record['sparkline']) if dict_record.get('sparkline') else None
        dict_record['contractAddresses'] = json.dumps(dict_record['contractAddresses']) if dict_record.get('contractAddresses') else None

        

        try:
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
            pool = sqlalchemy.create_engine(
                "mysql+pymysql://",
                creator=getconn,
            )
            print("Connection pool created")

            # Connect to the connection pool and execute queries
            with pool.connect() as db_conn:
                print("Connected to the database")

                # Create the coinranking table if it doesn't exist
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS coinranking_data (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        uuid VARCHAR(255) NOT NULL,
                        symbol VARCHAR(50),
                        name VARCHAR(100),
                        color VARCHAR(10),
                        iconUrl VARCHAR(255),
                        marketCap DECIMAL(36,18),
                        price DECIMAL(36,18),
                        listedAt INT,
                        tier INT,
                        `change` DECIMAL(10,2),
                        `rank` INT,
                        sparkline JSON,
                        lowVolume BOOLEAN,
                        coinrankingUrl VARCHAR(255),
                        Volume24h DECIMAL(36,18),
                        btcPrice DECIMAL(36,18),
                        contractAddresses JSON,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
                db_conn.execute(text(create_table_query))
                print("coinranking_data table created or already exists.")

                # Write into Table
                insert_statement = """
                INSERT INTO coinranking_data (
                    uuid, symbol, name, color, iconUrl, marketCap, price, listedAt, tier,
                    `change`, `rank`, sparkline, lowVolume, coinrankingUrl, Volume24h, btcPrice, created_at
                ) VALUES (
                    :uuid, :symbol, :name, :color, :iconUrl, :marketCap, :price, :listedAt, :tier,
                    :change, :rank, :sparkline, :lowVolume, :coinrankingUrl, :Volume24h, :btcPrice, :created_at
                )
                """
                
                
                print(f"Dict before insertion in SQL: {dict_record}")
                
                
                # Execute the insert statement with the provided dict_record
                db_conn.execute(text(insert_statement), dict_record)
                print("Data inserted successfully!")
                db_conn.close()
                print("Connector closed")

        except SQLAlchemyError as e:
            logging.exception(f"SQLAlchemy Error: {e}")
        except Exception as e:
            logging.exception(f"Error: {e}")
            
    # get the message from the event that triggered this run
    t_record = base64.b64decode(cloud_event.data["message"]["data"])
    print(f"Raw Pub/Sub message: {cloud_event.data['message']['data']}")

    
    # just make sure that it's formated as utf-8 string
    str_record = str(t_record,'utf-8')
    print(f"Formatted message: {str_record}")

    # Start the ingestion process
    ingest_to_db(str_record)
    print("Ingest to DB function completed")
