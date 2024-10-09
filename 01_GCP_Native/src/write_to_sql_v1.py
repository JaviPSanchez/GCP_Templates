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

# Instance connection name
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
            print(f"Raw string before JSON load: {str_record}")
            # Replace single quotes with double quotes to make it valid JSON format
            str_record = str_record.replace("'", '"')
            # Replace Python None with JSON null
            str_record = str_record.replace('None', 'null')
            # Replace Python True/False with JSON true/false
            str_record = str_record.replace('True', 'true')
            str_record = str_record.replace('False', 'false')
            dict_record = json.loads(str_record)
            print(f"JSON loaded!: {dict_record}")
            
            print(f"Preparing Json....")
                    # Convert 'date_added' and 'date_launched' to MySQL datetime format
            if dict_record.get('date_added'):
                dict_record['date_added'] = datetime.strptime(dict_record['date_added'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
        
            if dict_record.get('date_launched'):
                dict_record['date_launched'] = datetime.strptime(dict_record['date_launched'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
            # Prepare the data (modify dict_record keys to match the columns)
            dict_record = {
                'id': dict_record['id'],
                'name': dict_record['name'],
                'symbol': dict_record['symbol'],
                'category': dict_record['category'],
                'description': dict_record['description'],
                'slug': dict_record['slug'],
                'logo': dict_record['logo'],
                'subreddit': dict_record.get('subreddit', ''),
                'notice': dict_record.get('notice', ''),
                'tags': json.dumps(dict_record.get('tags', [])),
                'tag_names': json.dumps(dict_record.get('tag-names', [])),
                'tag_groups': json.dumps(dict_record.get('tag-groups', [])),
                'website_url': dict_record['urls']['website'][0] if dict_record['urls']['website'] else None,
                'twitter_url': dict_record['urls']['twitter'][0] if dict_record['urls']['twitter'] else None,
                'message_board_url': dict_record['urls']['message_board'][0] if dict_record['urls']['message_board'] else None,
                'chat_url': dict_record['urls']['chat'][0] if dict_record['urls']['chat'] else None,
                'facebook_url': dict_record['urls']['facebook'][0] if dict_record['urls']['facebook'] else None,
                'explorer_url': dict_record['urls']['explorer'][0] if dict_record['urls']['explorer'] else None,
                'reddit_url': dict_record['urls']['reddit'][0] if dict_record['urls']['reddit'] else None,
                'technical_doc_url': dict_record['urls']['technical_doc'][0] if dict_record['urls']['technical_doc'] else None,
                'source_code_url': dict_record['urls']['source_code'][0] if dict_record['urls']['source_code'] else None,
                'announcement_url': dict_record['urls']['announcement'][0] if dict_record['urls']['announcement'] else None,
                'platform': dict_record.get('platform'),
                'date_added': dict_record['date_added'],
                'twitter_username': dict_record['twitter_username'],
                'is_hidden': dict_record['is_hidden'],
                'date_launched': dict_record['date_launched'],
                'contract_address': json.dumps(dict_record.get('contract_address', [])),
                'self_reported_circulating_supply': dict_record['self_reported_circulating_supply'],
                'self_reported_tags': json.dumps(dict_record.get('self_reported_tags', [])),
                'self_reported_market_cap': dict_record['self_reported_market_cap'],
                'infinite_supply': dict_record['infinite_supply']
            }
            
            print(f"Done!!!")
            print(" ")
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

                # Insert data into the coin_data table
                insert_statement = """
                INSERT INTO coin_data (
                    id, name, symbol, category, description, slug, logo, subreddit, notice, 
                    tags, tag_names, tag_groups, website_url, twitter_url, message_board_url, 
                    chat_url, facebook_url, explorer_url, reddit_url, technical_doc_url, 
                    source_code_url, announcement_url, platform, date_added, twitter_username, 
                    is_hidden, date_launched, contract_address, self_reported_circulating_supply, 
                    self_reported_tags, self_reported_market_cap, infinite_supply
                ) VALUES (
                    :id, :name, :symbol, :category, :description, :slug, :logo, :subreddit, :notice, 
                    :tags, :tag_names, :tag_groups, :website_url, :twitter_url, :message_board_url, 
                    :chat_url, :facebook_url, :explorer_url, :reddit_url, :technical_doc_url, 
                    :source_code_url, :announcement_url, :platform, :date_added, :twitter_username, 
                    :is_hidden, :date_launched, :contract_address, :self_reported_circulating_supply, 
                    :self_reported_tags, :self_reported_market_cap, :infinite_supply
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
    
    # get the message from the event that triggered this run
    t_record = base64.b64decode(cloud_event.data["message"]["data"])
    print(f"Raw Pub/Sub message: {cloud_event.data['message']['data']}")

    # just make sure that it's formated as utf-8 string
    str_record = str(t_record, 'utf-8')
    print(f"Formatted message: {str_record}")

    # Start the ingestion process
    ingest_to_db(str_record)
    print("Ingest to DB function completed")
