
import os
import zipfile
from pathlib import Path
import pandas as pd
import csv # Import the csv module
import shutil
from dagster import asset, get_dagster_logger, AssetExecutionContext
import subprocess # Import subprocess to run shell commands

import pandas as pd
import matplotlib.pyplot as plt
import google.cloud.bigquery
from google.cloud import bigquery
from dagster_gcp import BigQueryResource

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
    full_path = os.path.abspath(local_path) 
    logger.info(f"Full path for local data directory: {full_path}")
       
    #remove data dir if exists and recreate
    if local_path.exists() and local_path.is_dir():
        shutil.rmtree(local_path) # Recursively deletes the directory and its contents

    local_path.mkdir(parents=True, exist_ok=True)

    try:
        import kaggle
        kaggle.api.authenticate() 
        logger.info(f"Downloading Kaggle dataset: {dataset_name} to {local_path}")
        
        # change the "local_path_str" to hard-coded "data" as dagster failed to find the path
        kaggle.api.dataset_download_files(
            dataset_name, 
            path=local_path_str, 
            unzip=True
        )
        logger.info(f"Successfully downloaded {dataset_name} to {os.path.abspath(local_path_str)}")

        downloaded_zip_file = None        
        for f in local_path.iterdir():
            
            if f.name == "kaggle_dataset":
                downloaded_zip_file = f
                break
            if f.suffix == ".zip":
                downloaded_zip_file = f
                break
            
        logger.info(f"Full path for local data directory: {full_path}")
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
    
    if not kaggle_dataset.is_dir():
        raise FileNotFoundError(f"Kaggle dataset input directory not found: {kaggle_dataset}")
    
    logger.info(f"Processing Kaggle dataset at: {kaggle_dataset}")
    
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

@asset
def transform_kaggle_data(context, load_kaggle_data): # Depends on the Meltano load completing
    """
    Invokes dbt to run transformations on the loaded data in BigQuery.
    This asset depends on 'load_kaggle_data' to ensure raw data is available.
    """
    logger.info("Starting dbt transformations (dbt run)...")

    try:
           
        # Execute the dbt run command
        result = subprocess.run(
            ['dbt', 'run'],
            capture_output=True,
            text=True,
            check=True,
            cwd='../transform_olist'
        )

        logger.info("dbt run command executed successfully.")
        logger.info(f"dbt stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"dbt stderr (if any):\n{result.stderr}")

    except FileNotFoundError:
        logger.error("dbt command not found. Ensure dbt CLI is installed and in your PATH.")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt command failed with exit code {e.returncode}.")
        logger.error(f"dbt stdout:\n{e.stdout}")
        logger.error(f"dbt stderr:\n{e.stderr}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while running dbt: {e}")
        raise

    logger.info("dbt transformations completed.")


@asset
def test_transform_data(context, transform_kaggle_data): # Depends on the dbt run completing
    """
    Invokes dbt to run tests on the transformed data.
    This asset depends on 'transform_kaggle_data' to ensure models are built.
    """

    logger.info("Starting dbt tests (dbt test)...")
    try:
        result_test = subprocess.run(
            ['dbt', 'test'],
            capture_output=True,
            text=True,
            check=True,
            cwd='../transform_olist'
        )
        logger.info("dbt test command executed successfully.")
        logger.info(f"dbt test stdout:\n{result_test.stdout}")
        if result_test.stderr:
            logger.warning(f"dbt test stderr (if any):\n{result_test.stderr}")
    except FileNotFoundError:
        logger.error("dbt command not found. Ensure dbt CLI is installed and in your PATH.")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt test command failed with exit code {e.returncode}.")
        logger.error(f"dbt test stdout:\n{e.stdout}")
        logger.error(f"dbt test stderr:\n{e.stderr}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during dbt test: {e}")
        raise
    logger.info("dbt tests completed.")
    
    
@asset(deps=["test_transform_data"])
def sales_distribution(context: AssetExecutionContext, bigquery: BigQueryResource) -> pd.DataFrame:
    """
    Reads data from 'your_dataset.your_table_1' in BigQuery into a Pandas DataFrame.
    """
    table_id = "olist.fact_sales_distribution"  # Replace with your actual dataset and table name
    context.log.info(f"Fetching data from BigQuery table: {table_id}")

    query = f"SELECT * FROM `{bigquery.project}.{table_id}` "
    
    try:
        
        with bigquery.get_client() as client:
            df = client.query(query).to_dataframe()
        
        df = client.query(query).to_dataframe()
        context.log.info(f"Successfully loaded {len(df)} rows from {table_id}")
        return df
    except Exception as e:
        context.log.error(f"Error reading from BigQuery table {table_id}: {e}")
        raise
    
@asset
def plot_dataframe_bar_chart(context: AssetExecutionContext, sales_distribution: pd.DataFrame):
    """
    Generates a bar chart from the 'my_first_bigquery_table' DataFrame
    and saves it as a PNG file.

    Assumes 'my_first_bigquery_table' has suitable columns for plotting.
    You'll need to customize 'x_column' and 'y_column'.
    """
    df = sales_distribution # The input DataFrame is automatically passed by Dagster

    # --- Customize these based on your DataFrame's columns ---
    x_column = "sales_bucket"  # Replace with the name of your categorical column for the x-axis
    y_column = "number_of_sellers_in_bucket"     # Replace with the name of your numerical column for the y-axis
    # --- End customization ---

    if x_column not in df.columns or y_column not in df.columns:
        context.log.error(f"Required columns '{x_column}' or '{y_column}' not found in DataFrame.")
        raise ValueError("Missing columns for plotting.")

    # Sort the DataFrame by the y_column for better visualization, if desired
    df_sorted = df.sort_values(by=y_column, ascending=False)

    plt.figure(figsize=(10, 6)) # Adjust figure size as needed
    plt.bar(df_sorted[x_column], df_sorted[y_column])

    plt.xlabel(x_column.replace('_', 'Bucket').title()) # Auto-capitalize and clean up label
    plt.ylabel(y_column.replace('_', 'Number of sellers').title())
    plt.title(f"Bar Chart of {y_column.replace('_', 'Number of sellers').title()} by {x_column.replace('_', 'Total_Sales').title()}")
    plt.xticks(rotation=45, ha='right') # Rotate x-axis labels if they overlap
    plt.tight_layout() # Adjust layout to prevent labels from being cut off

    # Define the output path for the plot image
    # Construct a path within the run's storage directory
    # context.instance.storage_directory() gives the base storage root
    # We then create a subdirectory for runs, the specific run_id, and the asset key for organization
    output_dir = os.path.join(
        context.instance.storage_directory(),
        "runs",
        context.run_id,
        context.asset_key.path[-1] # This gets the asset's name (e.g., "plot_dataframe_bar_chart")
    )
    
    os.makedirs(output_dir, exist_ok=True) # Ensure the directory exists
    plot_path = os.path.join(output_dir, f"{context.asset_key.path[-1]}_bar_chart.png")

    plt.savefig(plot_path)
    context.log.info(f"Bar chart saved to: {plot_path}")
    plt.close() # Close the plot to free up memory

    # You can optionally return the path or metadata if you want to track it
    # context.add_output_metadata({"plot_file": plot_path})