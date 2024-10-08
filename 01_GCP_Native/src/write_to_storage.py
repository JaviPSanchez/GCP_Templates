print("Loading Libraries....")
import os
import base64
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
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key_access_sql.json"

def write_to_storage(bucket_name, file_name, data):
    """Uploads a file to the bucket."""
    # Initialize a Cloud Storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a new blob (a file) in the bucket
    blob = bucket.blob(file_name)

    # Upload the data to the blob
    blob.upload_from_string(data)

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
    t_record = base64.b64decode(cloud_event["message"]["data"])
    str_record = str(t_record, 'utf-8')

    print(f"--------------------------------------------")
    print(f"Formatted message: {str_record}")
    print(f"--------------------------------------------")

    # Generate a filename using the timestamp or any unique identifier
    file_name = f"pubsub_message_{cloud_event["id"]}.txt"

    # Write the Pub/Sub message to Google Cloud Storage
    write_to_storage(bucket_name=BUCKET_NAME, file_name=file_name, data=str_record)

    print(f"--------------------------------------------")
    print(f"Message written to Cloud Storage as {file_name}")
    print(f"--------------------------------------------")

    return "Success"

# Mocked Pub/Sub event for local testing
def mock_pubsub_event():
    """Simulate a Pub/Sub event and test the function locally."""
    
    # Mocking a Pub/Sub message payload
    message_data = "This is a test message for Cloud Function."
    
    # Encoding the message in Base64, as Pub/Sub does by default
    encoded_message = base64.b64encode(message_data.encode('utf-8')).decode('utf-8')

    # Create a mock cloud_event object similar to what Pub/Sub would send
    mock_event = {
        "message": {
            "data": encoded_message,
            "messageId": "12345",
        },
        "id": "test-event-id",
        "source": "mock-pubsub",
        "type": "google.pubsub.topic.publish"
    }

    # Calling the actual function with the mock event
    print(f"Mocking Cloud Event: {mock_event}")
    write_to_database(mock_event)


if __name__ == "__main__":
    # Call the mock function to simulate the Pub/Sub event
    mock_pubsub_event()
