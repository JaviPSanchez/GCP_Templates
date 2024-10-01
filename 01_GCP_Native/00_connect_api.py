import os
import requests
import logging
from dotenv import load_dotenv


load_dotenv()

# Variables
BASE_URL = os.environ.get("BASE_URL")
API_KEY = os.getenv("API_KEY")


def connect_api():
    url = f"{BASE_URL}/everything?q=Apple&from=2024-09-01&sortBy=popularity&apiKey={API_KEY}"
    
    try:
        list_articles = []
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        for article in data['articles']:
            list_articles.append(article)
        print(list_articles[2])
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")

# Call the function
connect_api()