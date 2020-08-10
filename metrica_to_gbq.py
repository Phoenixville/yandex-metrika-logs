from google.cloud import bigquery, storage
from oauth2client.service_account import ServiceAccountCredentials

credentials = storage.Client.from_service_account_json("/media/phoenixville/USB DISK/GCP Credentials/evolveprototype-555950412a68.json")
bigquery_client = bigquery.Client(project="EvolvePrototype", credentials=credentials)


def gbq_job():
    dataset_id = 'evolveprototype'  # The dataset we named 'MyDataId'
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table('MyDataTable')  # Get table reference in dataset

    with open('my_dataframe.csv', 'rb') as source_file:  # Open CSV file
        conf = bigquery.LoadJobConfig()  # Initiate our GBQ configuration
        conf.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        conf.source_format = bigquery.SourceFormat.CSV  # Select the format of our source file
        conf.description = 'My Model Data'
        conf.autodetect = True

        # Run the GBQ job
        load_job = bigquery_client.load_table_from_file(
            source_file,
            table_ref,
            job_config=conf
        )

        load_job.result()