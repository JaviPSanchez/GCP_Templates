print("Loading Libraries....")
import os
import base64
import json
from dotenv import load_dotenv
from google.cloud import storage
import functions_framework
print("Done!!!")

# Load environment variables from .env file
load_dotenv(".env.local")

# Google Cloud Storage connection information
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Set the environment variable for Google Application Credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key_access_sql.json"

def write_to_storage(bucket_name, file_name, data):
    """Uploads a file to the bucket."""
    # Initialize a Cloud Storage client
    storage_client = storage.Client()

    # Get the bucket
    try:
        bucket = storage_client.bucket(bucket_name)
    except Exception as e:
        print(f"Error getting bucket: {e}")

    # Create a new blob (a file) in the bucket
    try:
        blob = bucket.blob(file_name)
    except Exception as e:
        print(f"Error Creting Blob file: {e}")
        
    # Upload the data to the blob
    try:
        blob.upload_from_string(json.dumps(data), content_type='application/json')
    except Exception as e:
        print(f"Error Uploading blob file: {e}")
        
    print(f"File {file_name} uploaded to {bucket_name}.")


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def write_to_database(cloud_event):
    """Triggered by Pub/Sub event and writes data to Cloud Storage.

    Args:
        cloud_event (CloudEvent): The event that triggered the function.
    """
    # Decode the Pub/Sub message data
    print(f"--------------------------------------------")
    print(f"Print Cloud Event: {cloud_event}")
    print(f"--------------------------------------------")
    t_record = base64.b64decode(cloud_event.data["message"]["data"])
    str_record = str(t_record, 'utf-8')

    print(f"--------------------------------------------")
    print(f"Formatted message: {str_record}")
    print(f"--------------------------------------------")
    
    dict_record = json.loads(str_record)
    print("------------------------------")
    print(f"JSON loaded!: {dict_record}")
    print("------------------------------")

    # Generate a filename using the timestamp from the dict_record
    file_name = f"{dict_record['status']['timestamp']}.json"

    # Write the Pub/Sub message to Google Cloud Storage
    write_to_storage(bucket_name=BUCKET_NAME, file_name=file_name, data=dict_record)

    print(f"--------------------------------------------")
    print(f"Message written to Cloud Storage as {file_name}")
    print(f"--------------------------------------------")

    return "Success"
