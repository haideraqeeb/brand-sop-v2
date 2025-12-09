import os
import json
import logging
import certifi
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongo_collection():
    """Helper to connect to MongoDB and return collection."""
    username = quote_plus(os.getenv("MONGO_USERNAME"))
    password = quote_plus(os.getenv("MONGO_PASSWORD"))
    cluster = os.getenv("MONGO_CLUSTER")
    app_name = os.getenv("MONGO_APPNAME")

    uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName={app_name}"
    mongo_kwargs = {"server_api": ServerApi('1'), "tlsCAFile": certifi.where()}

    # Allow disabling cert verification via env for local/dev troubleshooting
    if os.getenv("MONGO_ALLOW_INVALID_CERTS", "").lower() == "true":
        mongo_kwargs.pop("tlsCAFile", None)
        mongo_kwargs["tlsAllowInvalidCertificates"] = True

    client = MongoClient(uri, **mongo_kwargs)
    db = client["olambit"]
    return db["olambit-static-files"]

def upload(filename: str):
    """Upload a JSON file to MongoDB. Replaces existing document if filename already exists."""
    try:
        collection = get_mongo_collection()

        if not os.path.exists(filename):
            logger.error(f"File not found: {filename}")
            raise FileNotFoundError(f"{filename} not found.")

        logger.info(f"Reading file: {filename}")
        with open(filename, "r") as f:
            data = json.load(f)

        doc = {"filename": filename, "data": data}

        # Replace if exists, else insert
        result = collection.replace_one({"filename": filename}, doc, upsert=True)

        if result.matched_count > 0:
            logger.info(f"Replaced existing file: {filename}")
        else:
            logger.info(f"Uploaded new file: {filename}")

        return result

    except Exception as e:
        logger.exception(f"Error uploading {filename}: {e}")
        raise

def fetch(filename: str):
    """Fetch a JSON file from MongoDB by filename."""
    try:
        collection = get_mongo_collection()

        logger.info(f"Fetching file from DB: {filename}")
        doc = collection.find_one({"filename": filename})

        if not doc:
            logger.warning(f"No document found for: {filename}")
            return None

        logger.info(f"Fetched file successfully: {filename}")
        return doc["data"]

    except Exception as e:
        logger.exception(f"Error fetching {filename}: {e}")
        raise

# Example usage
if __name__ == "__main__":
    upload("formulae.json")
    data = fetch("formulae.json")
    logger.info(f"Fetched data preview: {str(data)[:200]}...")
