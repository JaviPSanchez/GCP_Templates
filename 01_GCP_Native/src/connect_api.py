import json
import requests
import logging
import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1

# Load environment variables from .env file
load_dotenv(".env.local")

# Environment variables
BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")
COIN = os.getenv("COIN")

PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")

TOPIC_ID = os.getenv("TOPIC_ID")

def pull_from_api(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    """Function to make an HTTP request to the API endpoint

    Args:
        event (_type_): Text message coming from the Scheduler
        context (_type_): _description_
    """
    logging.basicConfig(level=logging.INFO)

    # Check if variables are set
    if not BASE_URL:
        logging.error("Error: BASE_URL is not set.")
        return
    if not API_KEY:
        logging.error("Error: API_KEY is not set.")
        return
    if not COIN:
        logging.error("Error: COIN is not set.")
        return

    headers = {
        'x-access-token': API_KEY
    }

    data_json = None  # Initialize data_json

    try:
        # Make the API request
        response = requests.get(f"{BASE_URL}/coin/{COIN}", headers=headers)
        response.raise_for_status()  # Raise error for HTTP errors
        data_json = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {e}")
        return  # Exit the function if API request fails
    
    print(f"Json Data: {data_json["data"]["coin"]}")

    # Ensure data_json was successfully retrieved
    if data_json:
        try:
            print(f"Creating Publisher...")
            # Writing to our message queue Pub/Sub
            publisher = pubsub_v1.PublisherClient()
            print(f"Publisher: {publisher}")  
            # Creating Topic         
            topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
            print(f"Topic Path: {topic_path}")
            # Creating Message: Data coming from data
            message = json.dumps(data_json["data"]["coin"]).encode("utf-8")
            print(f"Message: {message}")
            # Send Message 
            future1 = publisher.publish(topic_path, message)
            print(f"Message published with result: {future1.result()}")
        except Exception as e:
            logging.error(f"Error publishing message: {e}")
    else:
        logging.error("No data received from API; nothing to publish.")

# Uncomment this line if you want to run the function directly
pull_from_api(None, None)
