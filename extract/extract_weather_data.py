import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv

S3_PREFIX = "weather_data_raw/"


def setup_logging():
    dirname = os.path.dirname(os.path.dirname(__file__))
    log_folder = os.path.join(dirname, "logs")
    os.makedirs(log_folder, exist_ok=True)
    logger = logging.getLogger("WeatherDataExtractor")
    if not logger.handlers:
        handler = logging.FileHandler(f"{log_folder}/weather_data_extractor.log")
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(funcName)s: %(lineno)d]"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)
    return logger


logger = setup_logging()


def load_environment_variables():
    """Load environment variables from .env file."""
    load_dotenv()
    WEATHERSTACK_API_KEY = os.getenv("WEATHERSTACK_API_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

    if not WEATHERSTACK_API_KEY or not S3_BUCKET_NAME:
        raise ValueError(
            "Environment variables WEATHERSTACK_API_KEY and S3_BUCKET_NAME must be set."
        )
    logger.info("Environment variables loaded successfully.")
    return WEATHERSTACK_API_KEY, S3_BUCKET_NAME


def prepare_request(api_key: str) -> str:
    api_url = f"https://api.weatherstack.com/current?access_key={api_key}&query=New Delhi, India"
    return api_url


def extract_data(api_url: str) -> dict | None:
    """Extracts weather data from the WeatherStack API.

    Args:
        api_url (str): The API endpoint to fetch weather data.

    Raises:
        re: requests.exceptions.RequestException: If there is an issue with the API request.
        e: Exception: For any other exceptions that may occur.

    Returns:
        dict|None: Returns the weather data as a dictionary if successful, otherwise None.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        return data
    except requests.exceptions.RequestException as re:
        logger.error(f"Error fetching data: {re}")
        raise re
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise e


def save_to_s3(data: dict, bucket_name: str, prefix_path: str):
    """Saves the extracted weather data to an S3 bucket in Parquet format.

    Args:
        data (dict): The weather data to be saved.
        bucket_name (str): The name of the S3 bucket where data will be saved.
        prefix_path (str): The prefix path in the S3 bucket where the file will be stored.

    Raises:
        NoCredentialsError: If AWS credentials are not found.
        PartialCredentialsError: If AWS credentials are incomplete.
        Exception: For any other exceptions that may occur during the save operation.
    """
    s3 = boto3.client("s3")
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    file_name = f"{prefix_path}{date_str}.parquet.gzip"
    try:
        df = pd.DataFrame([data])
        compressed_parquet = df.to_parquet(index=False, compression="gzip")
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=compressed_parquet)
        logger.info(
            f"Data successfully saved to S3 bucket {bucket_name} as {file_name}"
        )
    except NoCredentialsError:
        logger.error(
            "AWS credentials not found. Please configure your AWS credentials."
        )
        raise NoCredentialsError("AWS credentials not found.")
    except PartialCredentialsError:
        logger.error("Incomplete AWS credentials provided.")
        raise PartialCredentialsError("Incomplete AWS credentials provided.")
    except Exception as e:
        logger.error(f"Error saving data to S3: {e}")
        raise e


def main():
    try:
        logger.info("Starting the weather data extraction process.")
        WEATHERSTACK_API_KEY, S3_BUCKET_NAME = load_environment_variables()
        api_url = prepare_request(WEATHERSTACK_API_KEY)
        weather_data = extract_data(api_url)
        if not weather_data:
            raise ValueError("No data received from the API.")
        save_to_s3(weather_data, S3_BUCKET_NAME, S3_PREFIX)
    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
