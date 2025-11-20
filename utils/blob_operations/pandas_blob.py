from azure.storage.blob import BlobServiceClient
import pandas as pd
import json
from io import StringIO

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Fetch required environment variables for Azure Blob Storage
datasource = os.getenv("blob_data_source")
connection_string = os.getenv("blob_connection_string")
container_name = os.getenv("blob_container_name")

def append_file_to_blob(df, file_path, user_info={}, container_name=container_name):
    """
    Appends data from a DataFrame to an existing file in Azure Blob Storage.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to append.
        file_path (str): The file path in the blob storage to which the data will be appended.
        user_info (dict, optional): A dictionary containing user-specific information for filtering. Defaults to {}.
        container_name (str, optional): The container name in Azure Blob Storage. Defaults to `container_name` from environment.

    Returns:
        None
    """
    # Retrieve previously ingested data from the blob
    previous_version_df = get_ingested_data(file_path)
    
    # Append data with or without user-specific filtering
    if not user_info:
        # No user info provided, simply append data
        new_version_df = previous_version_df.append(df)
    else:
        # Filter data based on user credentials before appending
        nonuser_previous_version_df = previous_version_df[previous_version_df['user_cred'] != user_info['user']['email']].copy()
        new_version_df = nonuser_previous_version_df.append(df)
    
    # Save the new combined DataFrame locally
    new_version_df.to_csv(file_path, index=False)
    
    # Set up Azure Blob client to upload the file
    blob_name = file_path
    local_file_name = file_path
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Upload the file to Azure Blob Storage
    with open(local_file_name, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

def update_data_to_the_blob(file_path, user_info, column_name, column_value, 
                            connection_string=connection_string, container_name=container_name):
    """
    Updates a specific column's value for a user's data in an existing blob file.

    Args:
        file_path (str): The file path in the blob storage to update.
        user_info (dict): A dictionary containing user-specific information for identifying the data to update.
        column_name (str): The column to update in the user's data.
        column_value (any): The new value to set for the specified column.
        connection_string (str, optional): Azure Blob Storage connection string. Defaults to `connection_string`.
        container_name (str, optional): The Azure Blob Storage container name. Defaults to `container_name`.

    Returns:
        bool: True if update is successful, False otherwise.
    """
    try:
        # Retrieve previously ingested data from the blob
        previous_version_df = get_ingested_data(file_path)
        
        # Separate the data based on user credentials
        nonuser_previous_version_df = previous_version_df[previous_version_df['user_cred'] != user_info['user']['email']].copy()
        user_previous_version_df = previous_version_df[previous_version_df['user_cred'] == user_info['user']['email']].copy()
        
        # Update the specified column for the user's data
        user_previous_version_df[column_name] = column_value
        
        # Combine the updated user data with the non-user data
        new_version_df = pd.concat([user_previous_version_df, nonuser_previous_version_df])
        
        # Save the new version to a CSV file
        new_version_df.to_csv(file_path, index=False)
        
        # Upload the updated file to Azure Blob Storage
        save_file_to_blob(new_version_df, file_path, connection_string=connection_string, container_name=container_name)
        return True
    except Exception as e:
        # In case of error, return False
        return False

def save_file_to_blob(df, file_path, datasource=datasource,
    connection_string=connection_string,
    container_name=container_name):
    """
    Saves the DataFrame to a CSV file and uploads it to Azure Blob Storage.

    Args:
        df (pd.DataFrame): The DataFrame to save and upload.
        file_path (str): The path where the CSV file will be saved locally.
        datasource (str, optional): A description of the data source. Defaults to `datasource`.
        connection_string (str, optional): The Azure Blob Storage connection string. Defaults to `connection_string`.
        container_name (str, optional): The Azure Blob Storage container name. Defaults to `container_name`.

    Returns:
        None
    """
    # Save the DataFrame as a CSV file
    df.to_csv(file_path, index=False)
    
    # Set up the Azure Blob client to upload the file
    blob_name = file_path
    local_file_name = file_path
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Upload the CSV file to Azure Blob Storage
    with open(local_file_name, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

def get_ingested_data(file_path, datasource=datasource, connection_string=connection_string, container_name=container_name):
    """
    Retrieves data from a blob storage container.

    Args:
        file_path (str): The path to the blob file.
        datasource (str, optional): Description of the data source. Defaults to `datasource`.
        connection_string (str, optional): The Azure Blob Storage connection string. Defaults to `connection_string`.
        container_name (str, optional): The name of the blob storage container. Defaults to `container_name`.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the ingested data.
    """
    # Create a BlobServiceClient using the provided connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string, connection_verify=False)

    # Get the container client for the specified container
    container_client = blob_service_client.get_container_client(container_name)

    # Get the blob client for the specified file
    blob_client = container_client.get_blob_client(file_path)

    # Download the contents of the blob
    blob_data = blob_client.download_blob().readall().decode("utf-8")

    # Read the blob data into a pandas DataFrame
    ingested_df = pd.read_csv(StringIO(blob_data))

    # Return the ingested data as a DataFrame
    return ingested_df