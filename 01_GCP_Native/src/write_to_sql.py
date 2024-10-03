print("Loading Libraries....")
import os
import logging
import base64
import sqlalchemy
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import functions_framework
from google.cloud.sql.connector import Connector

print("Done!!!")

# Load environment variables from .env file
load_dotenv(".env.local")

# Set the environment variable for Google Application Credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key_access_sql.json"

# Configure logging
logging.basicConfig(level=logging.INFO)  # You can change this to DEBUG for more detailed logs

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
                logging.info(f"Connecting to instance: {INSTANCE_CONNECTION_NAME}")
                print(f"Connecting to instance: {INSTANCE_CONNECTION_NAME}")
                return conn
            
        
            
            # Load the JSON string properly
            try:
                dict_record = json.loads(str_record)  # Load JSON string into a dictionary
                # Add current timestamp to dict_record
                dict_record['created_at'] = datetime.now()
                print(f"dictionary: {dict_record}")  # Print the decoded JSON dictionary
            except json.JSONDecodeError as e:
                logging.error(f"JSON Decode Error: {e}")
                return  # Exit the function if the JSON is invalid
            
            # Fix any `None` values in the sparkline array by replacing them with `null`
            dict_record['sparkline'] = [value if value is not None else 'null' for value in dict_record['sparkline']]

            # Create a connection pool with SQLAlchemy
            pool = sqlalchemy.create_engine(
                "mysql+pymysql://",
                creator=getconn,
            )
            print("Connection pool created")

            # Connect to the connection pool and execute queries
            with pool.connect() as db_conn:
                print("Connected to the database")

                # Execute a sample query to show databases and assign result
                result = db_conn.execute(text("SHOW DATABASES;"))
                logging.info("Showing Databases...")
                print("Showing Databases...")

                # Fetch and display the databases
                databases = result.fetchall()
                for db in databases:
                    print(f"Database: {db[0]}")

                # Create the coinranking table if it doesn't exist
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS coinranking_data (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        symbol VARCHAR(10) NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        color VARCHAR(7),
                        iconUrl VARCHAR(255),
                        marketCap BIGINT,
                        price DECIMAL(30, 10),
                        listedAt BIGINT,
                        tier INT,
                        `change` DECIMAL(5, 2),
                        `rank` INT,
                        sparkline JSON,
                        lowVolume BOOLEAN,
                        coinrankingUrl VARCHAR(255),
                        volume_24h BIGINT,
                        btcPrice DECIMAL(18, 10),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
                db_conn.execute(text(create_table_query))
                print("coinranking_data table created or already exists.")
                    
                # Write into Table
                insert_statement = """
                INSERT INTO coinranking_db.coinranking_data (
                    symbol,
                    name,
                    color,
                    iconUrl,
                    marketCap,
                    price,
                    listedAt,
                    tier,
                    `change`,
                    `rank`,
                    sparkline,
                    lowVolume,
                    coinrankingUrl,
                    24hVolume,
                    btcPrice,
                    created_at
                ) VALUES (
                    :symbol,
                    :name,
                    :color,
                    :iconUrl,
                    :marketCap,
                    :price,
                    :listedAt,
                    :tier,
                    :change,
                    :rank,
                    :sparkline,
                    :lowVolume,
                    :coinrankingUrl,
                    :24hVolume,
                    :btcPrice,
                    :created_at
                )
                """
                
                # Rename the key '24hVolume' to 'volume_24h' while keeping its position
                dict_record['volume_24h'] = dict_record['24hVolume']
                del dict_record['24hVolume']  # Remove the old key
                
                print(f"Dict avant insertion in SQL: {dict_record}")
                
                
                
                # Execute the insert statement with the provided dict_record
                db_conn.execute(text(insert_statement), dict_record)
                print("Data inserted successfully!")


        except SQLAlchemyError as e:
            logging.error(f"SQLAlchemy Error: {str(e)}")

        except Exception as e:
            logging.error(f"Error: {str(e)}")

        finally:
            # Ensure db_conn is closed if it was opened
            if db_conn is not None:
                db_conn.close()
                print("Database connection closed")

            # Properly close the connector to prevent aiohttp errors
            connector.close()
            print("Connector closed")
            
    # get the message from the event that triggered this run
    t_record = base64.b64decode(cloud_event.data["message"]["data"])
    print(f"Raw Pub/Sub message: {cloud_event.data['message']['data']}")

    
    # just make sure that it's formated as utf-8 string
    str_record = str(t_record,'utf-8')
    print(f"Formatted message: {str_record}")

    # Start the ingestion process
    ingest_to_db(str_record)
    print("Ingest to DB function completed")


# write_to_database(cloud_event)