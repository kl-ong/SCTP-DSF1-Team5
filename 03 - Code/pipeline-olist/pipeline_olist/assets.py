
import os
import zipfile
from pathlib import Path
import pandas as pd
import csv # Import the csv module
import shutil
from dagster import asset, get_dagster_logger
import subprocess # Import subprocess to run shell commands


logger = get_dagster_logger()

@asset
def kaggle_dataset(context) -> Path:
    """
    Downloads a specified Kaggle dataset and extracts its contents.

    Configurable parameters:
    - dataset_name (str): The full name of the Kaggle dataset (e.g., 'owner/dataset-name').
    - local_path (str): The local directory where the dataset will be downloaded and extracted.
    - unzip (bool): Whether to unzip the downloaded file (Kaggle datasets are often zipped).
    """
    # This diagnostic check will help confirm if config is missing
    if context.op_config is None:
        logger.error("context.op_config is None. This means no static_config or run config was provided to the asset.")
        # Provide fallback default values to prevent the AttributeError
        dataset_name = "olistbr/brazilian-ecommerce" # Default if no config provided
        local_path_str = "../data" # Default if no config provided
        unzip_data = True # Default if no config provided
        logger.warning(f"Using fallback default values for kaggle_dataset due to missing config: dataset_name={dataset_name}, local_path={local_path_str}, unzip={unzip_data}")
    else:
        dataset_name = context.op_config.get("dataset_name") # No default here as we assume config is present
        local_path_str = context.op_config.get("local_path")
        unzip_data = context.op_config.get("unzip")

    local_path = Path(local_path_str)

    #remove data dir if exists and recreate
    if local_path.exists() and local_path.is_dir():
        shutil.rmtree(local_path) # Recursively deletes the directory and its contents

    local_path.mkdir(parents=True, exist_ok=True)

    try:
        import kaggle
        kaggle.api.authenticate() 
        logger.info(f"Downloading Kaggle dataset: {dataset_name} to {local_path}")
        
        kaggle.api.dataset_download_files(
            dataset_name, 
            path=local_path_str, 
            unzip=True
        )
        logger.info(f"Successfully downloaded {dataset_name} to {local_path}")

        downloaded_zip_file = None        
        for f in local_path.iterdir():
            
            if f.suffix == ".zip":
                downloaded_zip_file = f
                break
        
        if downloaded_zip_file:
            if unzip_data:
                logger.info(f"Unzipping {downloaded_zip_file}...")
                with zipfile.ZipFile(downloaded_zip_file, 'r') as zip_ref:
                    zip_ref.extractall(local_path)
                logger.info(f"Successfully unzipped to {local_path}")
                os.remove(downloaded_zip_file)
            else:
                logger.info("Skipping unzip as 'unzip' config is False.")
        else:
            logger.warning(f"No zip file found for dataset '{dataset_name}' in '{local_path}'. "
                           "It might have been downloaded directly (e.g., competition data) or an issue occurred.")

    except Exception as e:
        logger.error(f"Error downloading Kaggle dataset {dataset_name}: {e}")
        raise

    return local_path

@asset
def processed_kaggle_data(context, kaggle_dataset: Path) -> list[Path]:
    """
    Finds all CSV files in the downloaded Kaggle dataset,
    cleans newline and carriage return characters from their fields,
    and writes the cleaned data to new files.
    Returns a list of Paths to the newly created cleansed CSV files.
    """
    csv_files = list(kaggle_dataset.glob("*.csv"))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {kaggle_dataset} for cleansing.")
        return [] # Return an empty list if no CSVs are found

    cleansed_file_paths = []
    logger.info(f"Found {len(csv_files)} CSV file(s) for cleansing in {kaggle_dataset}.")

    for input_file_path in csv_files:
        logger.info(f"Starting cleansing for: {input_file_path}")

        # Define the output path for the cleansed file
        base_name, ext = os.path.splitext(input_file_path)
        output_file_path = Path(f"{base_name}_cleansed{ext}")  

        # Implement the cleaning algorithm directly
        try:
            with open(input_file_path, 'r', encoding='utf-8-sig', newline='') as infile, \
                 open(output_file_path, 'w', encoding='utf-8-sig', newline='') as outfile:
                
                reader = csv.reader(infile, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
                writer = csv.writer(outfile, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)

                for row in reader:
                    cleaned_row = [str(field).replace('\n', ' ').replace('\r', ' ').strip('\ufeff') for field in row]
                    writer.writerow(cleaned_row)

            logger.info(f"Successfully cleaned and saved: {output_file_path}")
            cleansed_file_paths.append(output_file_path)
            os.remove(input_file_path)
            
        except Exception as e:
            logger.error(f"Error cleaning CSV file {input_file_path}: {e}")
            # Optionally, you might want to raise the exception or log and continue
            # For robustness, we'll log and continue to process other files.
            # If you want to halt the job on any single file failure, re-add 'raise' here.

    logger.info(f"Finished cleansing. Total {len(cleansed_file_paths)} file(s) cleansed.")
    return cleansed_file_paths # Return the list of paths to all cleansed files

# --- NEW ASSET TO INVOKE MELTANO RUN ---

@asset
def load_kaggle_data(context, processed_kaggle_data: list[Path]): # <--- KEY CHANGE: Added dependency
    """
    Invokes Meltano to run the tap-csv to target-bigquery pipeline.
    This asset depends on 'processed_kaggle_data' to ensure CSV files are ready.
    """
    # logger.info(f"Received cleansed CSV paths: {cleansed_csv_paths}")
    logger.info("Starting Meltano data load (tap-csv to target-bigquery)...")

    try:
        # It's good practice to run Meltano from your project root.
        # Ensure your current working directory when this asset runs is your Meltano project root.
        # If not, you might need to adjust the 'cwd' parameter in subprocess.run.
        # For example: subprocess.run(['meltano', ...], check=True, cwd='/path/to/your/meltano/project')
        
        # Execute the Meltano command
        # Using 'check=True' will raise a CalledProcessError if the command returns a non-zero exit code.
        result = subprocess.run(
            ['meltano', 'run', 'tap-csv', 'target-bigquery'],
            capture_output=True, # Capture stdout and stderr
            text=True,           # Decode stdout/stderr as text
            check=True,          # Raise an exception for non-zero exit codes
            cwd='../load-olist'
        )

        logger.info("Meltano command executed successfully.")
        logger.info(f"Meltano stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Meltano stderr (if any):\n{result.stderr}")

    except FileNotFoundError:
        logger.error("Meltano command not found. Ensure Meltano CLI is installed and in your PATH.")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"Meltano command failed with exit code {e.returncode}.")
        logger.error(f"Meltano stdout:\n{e.stdout}")
        logger.error(f"Meltano stderr:\n{e.stderr}")
        raise # Re-raise the exception to indicate asset failure
    except Exception as e:
        logger.error(f"An unexpected error occurred while running Meltano: {e}")
        raise

    logger.info("Meltano data load completed.")