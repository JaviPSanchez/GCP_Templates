import json
import requests
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")

def connect_api():
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
        data = response.json()
        print(data["data"]['coins'][0])
    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {e}")

connect_api()

