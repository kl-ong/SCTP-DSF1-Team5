-----------------------------------------------------------------------------------------------------------------------------------
Objective: To provide a regular report (weekly or monthly, to be configured on dagster) to Olist eCommerce company on their sales status.
-----------------------------------------------------------------------------------------------------------------------------------
Proposed to build a dagster pipeline with the following stages
1. Ingest from Kaggle olistbr/brazilian-ecommerce dataset and unzip
2. Prepare the olistbr/brazilian-ecommerce CSVs for ingestion by cleansing it.
3. Use meltano to ingest to BigQuery
4. Use dbt to transform dataset to STAR schema coupled with dbt test (uniqueness, non-nullable, foreign keys)
5. Use Great Expectations to further perform data validation (TBC test cases)
6. TBC perform dataanalysis and reporting with Pandas and Matplot, etc.
-----------------------------------------------------------------------------------------------------------------------------------

![Data Pipeline](./03%20-%20Code/Assets/pipeline.png)

```
cd SCTP-DSF1-Team5/03 - Code/
```

-----------------------------------------------------------------------------------------------------------------------------------
Building the data pipeline using Dagster  
/pipeline-olist
-----------------------------------------------------------------------------------------------------------------------------------
Create a Kaggle API Token (kaggle.json) and place it under /home/youruser/.kaggle/kaggle.json
```
conda activate elt
# check if kaggle is in elt environmant
conda list kaggle
conda install -c conda-forge kaggle
conda activate elt
cd pipeline-olist
```
If run is successful, the kaggle_dataset downloaded (in hidden directory) is in 
/Users/taysk/sctp/Project/SCTP-DSF1-Team5/03 - Code/pipeline-olist/.tmp_dagster_home_wbs0lnyc/storage

`/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/pipeline-olist/pipeline_olist/assets.py  `
Implment assets.py to download datasets from kaggle and cleansing

`/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/pipeline-olist/pipeline_olist/definitions.py  `
Implment definitions to load all assets from assets.py

```
dagster dev
```

-----------------------------------------------------------------------------------------------------------------------------------
Building the data loading of Kaggle dataset to BigQuery using Meltano  
/load-olist
-----------------------------------------------------------------------------------------------------------------------------------
```
conda activate elt
cd load-olist
```

`/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/load-olist/meltano.yml  `
Add and populate extractor information  
```
meltano add extractor tap-csv 
```

To test extractor
```
meltano invoke tap-csv
```

Add and populate loader information, get ready your Big Query Service account key  
Your BigQuery service account key should be located at `/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/credentials/<your bigquery service account key>.json`. 
```
meltano add loader target-bigquery
```

Run your loading
```
meltano run tap-csv target-bigquery
```

-----------------------------------------------------------------------------------------------------------------------------------
Building data transformation of Kaggle dataset using dbt  
/transform_olist
-----------------------------------------------------------------------------------------------------------------------------------
```
conda activate dwh
cd transform_olist
```

`/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/transform_olist/dbt_project.yml  `
Verify the models are configured to +materialized: table

Your BigQuery service account key should be located at `/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/credentials/<your bigquery service account key>.json`.  

`/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/transform_olist/profiles.yml  `
Set keyfile to point to the path of your BigQuery service account credentials  
Set method to service-account for authentication to  BigQuery  
Set project to your BigQuery project id  


`/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/transform_olist/models/sources.yml  `
Set name of source to olist_raw (this is the source when we load with meltano)
Set database to your BigQuery project id
Set tables to the same name we used for loading with meltano

`/home/<your username>/SCTP/SCTP-DSF1-Team5/03 - Code/transform_olist/packages.yml  `
Add 
```
packages:
  - package: calogica/dbt_expectations
    version: [">=0.8.0", "<0.9.0"]
```

Install package(s)
```
dbt deps
```

Run your transformation
```
dbt run
```

test your transformation
```
dbt test
```

-----------------------------------------------------------------------------------------------------------------------------------
/Notebook
-----------------------------------------------------------------------------------------------------------------------------------

Set your BigQuery credentials environment path so that jupyter can use for authentication with BigQuery.
```
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```
