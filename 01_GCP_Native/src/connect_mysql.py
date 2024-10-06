import os
import logging
import sqlalchemy
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(".env.local")

# Set the environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key_access_sql.json"

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

def ingest_to_db():
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
                CREATE TABLE IF NOT EXISTS coin_data (
                    id INT PRIMARY KEY,
                    name VARCHAR(255),
                    symbol VARCHAR(10),
                    category VARCHAR(50),
                    description TEXT,
                    slug VARCHAR(100),
                    logo VARCHAR(255),
                    subreddit VARCHAR(255),
                    notice VARCHAR(255),
                    tags JSON,
                    tag_names JSON,
                    tag_groups JSON,
                    website_url VARCHAR(255),
                    twitter_url VARCHAR(255),
                    message_board_url VARCHAR(255),
                    chat_url VARCHAR(255),
                    facebook_url VARCHAR(255),
                    explorer_url VARCHAR(255),
                    reddit_url VARCHAR(255),
                    technical_doc_url VARCHAR(255),
                    source_code_url VARCHAR(255),
                    announcement_url VARCHAR(255),
                    platform VARCHAR(100),
                    date_added DATETIME,
                    twitter_username VARCHAR(255),
                    is_hidden BOOLEAN,
                    date_launched DATETIME,
                    contract_address JSON,
                    self_reported_circulating_supply DECIMAL(20, 10),
                    self_reported_tags JSON,
                    self_reported_market_cap DECIMAL(20, 10),
                    infinite_supply BOOLEAN
                );
            """
            db_conn.execute(text(create_table_query))
            print("coin_data table created or already exists.")
            
            
            # Execute a query to fetch all rows from the coinranking_data table
            result = db_conn.execute(text("SELECT * FROM coin_db.coin_data;"))
            print("Fetching data from coin_data...")

            # Fetch all results
            rows = result.fetchall()

            # Print out the rows
            for row in rows:
                print(row)
            
    except SQLAlchemyError as e:
        print(f"SQLAlchemy error occurred: {str(e)}")
        logging.error(f"SQLAlchemy Error: {str(e)}")

    except Exception as e:
        print(f"General error occurred: {str(e)}")
        logging.error(f"Error: {str(e)}")

    finally:
        # Ensure db_conn is closed if it was opened
        if db_conn is not None:
            db_conn.close()
            print("Database connection closed")

        # Properly close the connector to prevent aiohttp errors
        connector.close()
        print("Connector closed")

# Start the ingestion process
ingest_to_db()
print("Ingest to DB function completed")
