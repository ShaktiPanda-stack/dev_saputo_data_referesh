def read_parquet_from_blob(file_path, spark=None, folder_path=None, storage_account=None, container_name=None, sas_token=None):
    """
    Reads a Parquet file from Azure Blob Storage into a Spark DataFrame.

    Args:
        file_path (str): The name of the Parquet file to read from Azure Blob Storage.
        spark (SparkSession, optional): The Spark session to use for reading the Parquet file. Defaults to None.
        folder_path (str, optional): The folder within the container where the file is located. Defaults to None.
        storage_account (str, optional): The Azure Storage Account name. Defaults to None.
        container_name (str, optional): The name of the Azure Blob Storage container. Defaults to None.
        sas_token (str, optional): The SAS token for authentication with Azure Blob Storage. Defaults to None.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the Parquet file.

    Raises:
        ValueError: If any required arguments are missing or invalid.
    """
    # Validate input parameters to ensure they are provided correctly
    if not all([file_path, spark, storage_account, container_name, sas_token]):
        raise ValueError("All required parameters (file_path, spark, storage_account, container_name, sas_token) must be provided.")
    
    # If folder_path is provided, prepend it to the file_path to form the full path
    if folder_path is not None:
        file_path = f"{folder_path}/{file_path}"

    # Configure the Spark session with the SAS token for Azure Blob Storage access
    spark.conf.set(f"fs.azure.sas.{container_name}.{storage_account}.blob.core.windows.net", sas_token)

    # Construct the URL for accessing the Parquet file using the Azure Blob Storage protocol (wasbs://)
    # wasbs_url = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/{file_path}"
    wasbs_url = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/{file_path}"


    # Read the Parquet file into a Spark DataFrame
    df = spark.read.parquet(wasbs_url)

    # Return the DataFrame
    return df





def write_parquet_to_blob(df, file_path, spark=None, folder_path=None, storage_account=None, container_name=None, sas_token=None, mode="overwrite"):
    """
    Writes a PySpark DataFrame to Azure Blob Storage in Parquet format.

    Args:
        df (DataFrame): The PySpark DataFrame to write to Azure Blob Storage.
        file_path (str): The name of the output Parquet file.
        spark (SparkSession, optional): The Spark session to use for writing the DataFrame. Defaults to None.
        folder_path (str, optional): The folder inside the container to write the file. Defaults to None.
        storage_account (str, optional): The Azure Storage Account name. Defaults to None.
        container_name (str, optional): The Azure Blob Container name. Defaults to None.
        sas_token (str, optional): The SAS token for authentication. Defaults to None.
        mode (str, optional): The write mode, can be "overwrite", "append", "ignore", or "error". Defaults to "overwrite".

    Returns:
        None

    Raises:
        ValueError: If any required arguments are missing or invalid.
    """
    # Validate the required parameters to ensure they are provided correctly
    if not all([df, file_path, spark, storage_account, container_name, sas_token]):
        raise ValueError("All required parameters (df, file_path, spark, storage_account, container_name, sas_token) must be provided.")
    
    # Construct the full file path by prepending the folder path, if provided
    if folder_path:
        file_path = f"{folder_path}/{file_path}"
    else:
        file_path = file_path

    # Configure Spark session with the SAS token for Azure Blob Storage access
    spark.conf.set(f"fs.azure.sas.{container_name}.{storage_account}.blob.core.windows.net", sas_token)

    # Construct the URL for the Parquet file location using the Azure Blob Storage protocol (wasbs://)
    wasbs_url = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/{file_path}"

    # Write the DataFrame to Azure Blob Storage in Parquet format with the specified write mode
    df.write.mode(mode).parquet(wasbs_url)

    # Print a success message indicating where the data has been written
    print(f"Data successfully written to {wasbs_url}")