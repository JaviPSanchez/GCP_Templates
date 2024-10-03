import json
import requests
import logging
import os
from dotenv import load_dotenv
# Google environment
# from google.cloud import pubsub_v1

# Load environment variables from .env file
load_dotenv(".env.local")

# https://developers.coinranking.com/api/documentation
BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")

# def pull_from_api(event, context):
def pull_from_api():
    """Function to make an http request to the api endpoint

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

    headers = {
        'x-access-token': API_KEY
    }

    try:
        # Make the API request
        response = requests.get(f"{BASE_URL}/coins", headers=headers)
        response.raise_for_status()  # Raise error for HTTP errors
        data_json = response.json()
        print(data_json["data"]['coins'][0])
    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {e}")
        
    # Writing to our message queue PubSub
    # publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}` to tell
    # where should be written
    # topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    # Create the string
    # message = json.dumps(data_json).encode("utf-8")
    # future1 = publisher.publish(topic_path, message)
    # print(future1.result())

pull_from_api()

