To ingest the olist_order_reviews_dataset.csv using `dbt seed`, use the following code (from ChatGPT) to clean the csv file and then `dbt seed` the cleaned file. I tried to include `allow_quoted_newlines = true` for seeds in the `dbt_project.yml` but it doesn't work.

```python
import csv

input_file = 'olist_order_reviews_dataset.csv'
output_file = 'olist_order_reviews_dataset_cleaned.csv'

with open(input_file, 'r', encoding='utf-8', newline='') as infile, \
     open(output_file, 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.reader(infile, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    writer = csv.writer(outfile, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    
    for row in reader:
        cleaned_row = [field.replace('\n', ' ').replace('\r', ' ') if field else '' for field in row]
        writer.writerow(cleaned_row)

print("Cleaned CSV written to", output_file)
```

If creating the table directly in GCP using upload, then just enable the Quoted newlines as shown for the reviews.csv

![image](gcp_quoted_newlines.png)
