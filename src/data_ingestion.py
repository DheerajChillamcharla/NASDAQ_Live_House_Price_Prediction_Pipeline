import zipfile
import os
import json
import subprocess
import logging
import shutil

logger = logging.getLogger("data_ingestion.task")


def download_data(**kwargs):
    """
    Downloads a JSON file containing a download link from the NASDAQ data API,
    then uses that link to download a ZIP file containing the dataset.

    The function performs the following operations:
    1. Downloads a JSON file with a data download link using a curl command.
    2. Reads the JSON file to extract the download link for the ZIP file.
    3. Downloads the ZIP file using the extracted link via a curl command.

    No parameters.
    No return value.
    """


    logger.info("Starting download data task")
    # Run the curl command using subprocess
    # Create directories if they don't exist
    if not os.path.exists('../artifacts'):
        os.makedirs('../artifacts')
    if not os.path.exists('../data'):
        os.makedirs('../data')
    subprocess.run(['curl', '-o', os.path.join(os.path.dirname(__file__), '../artifacts/download_link.json'),
                    'https://data.nasdaq.com/api/v3/datatables/ZILLOW/DATA?qopts.export=true&api_key=yaXdjWK7YAbeWZMoqphn'])

    logger.info("Json file with download link to zip file downloaded")
    # Specify the path to your JSON file
    json_file_path = os.path.join(os.path.dirname(__file__), '../artifacts/download_link.json')

    # Open and read the JSON file
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)

    download_link = data["datatable_bulk_download"]["file"]["link"]

    # Run the curl command using subprocess
    subprocess.run(['curl', '-o', os.path.join(os.path.dirname(__file__), '../artifacts/zillow_data.zip'),
                    download_link])

    logger.info("Data zip file downloaded")


def extract_data():
    """
        Extracts a CSV file from a ZIP archive and moves it to a designated location.

        This function performs the following operations:
        1. Opens a ZIP file located in a subdirectory named 'artifacts' relative to the script's location.
        2. Extracts a CSV file to the same 'artifacts' directory.
        3. Moves the extracted CSV file to a subdirectory named 'data', renaming it to 'zillow_data.csv'.

        Note:
        - The function assumes there's only one CSV file in the ZIP archive. If there are multiple CSV files or a different file structure, modifications will be necessary.
        - The function uses a system command ('mv') to move the file, which might not be portable across all operating systems.

        No parameters.
        No return value.
        """
    # Get the current working directory
    current_directory = os.getcwd()
    zip_file_path = os.path.join(os.path.dirname(__file__), '../artifacts/zillow_data.zip')

    # Path where you want to extract the CSV file
    output_dir = current_directory

    logger.info("Starting zip file extraction")

    # Open the ZIP file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        # List the contents of the ZIP file (optional)
        zip_file_contents = zip_file.namelist()
        print("Contents of the ZIP file:", zip_file_contents)

        # Extract the CSV file (assuming there's only one CSV file in the ZIP)
        for file_name in zip_file_contents:
            if file_name.endswith('.csv'):
                zip_file.extract(file_name, os.path.join(os.path.dirname(__file__), '../artifacts/'))
                extracted_csv_path = output_dir + file_name
                print(f"Extracted CSV file: {extracted_csv_path}")
    logger.info(f"Extracted CSV file: {extracted_csv_path}")
    logger.info("zip file extraction done")

    # Move and rename the file
    shutil.move(os.path.join(os.path.dirname(__file__), '../artifacts/') + file_name,
                os.path.join(os.path.dirname(__file__), '../data/zillow_data.csv'))
    # serialized_data = pickle.dumps(df)
    #
    # return serialized_data


download_data()
extract_data()
