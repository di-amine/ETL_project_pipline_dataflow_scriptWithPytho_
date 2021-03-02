from google.cloud import bigquery

PROJECT = 'abel-traore49'
BUCKET = 'myb_amine22'
dataset='covid19_italy_amine'
table='covid19it'



    # Construct a BigQuery client object.
client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the destination table.
for region_code in range(6, 20):
    newtable = "abel-traore49.covid19_italy_amine.table_region_code_N"+str(region_code)



    job_config = bigquery.QueryJobConfig(allow_large_results=True, destination=newtable)

    sql = """
        SELECT *
        FROM `{}.{}.{}`
        WHERE region_code = {} ;
    """


        # Start the query, passing in the extra configuration.
    query_job = client.query(sql.format(PROJECT, dataset, table, region_code), job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(newtable))
        # [END bigquery_query_destination_table]

    client = bigquery.Client()
    table_id = "table_region_code_N"+str(region_code)
    filecsv = 'tablcovid'+str(region_code)+'.csv'

    destination_uri = "gs://{}/{}".format(BUCKET, filecsv)
    dataset_ref = bigquery.DatasetReference(PROJECT, dataset)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported table {}:{}.{} to file CSV --> {}".format(PROJECT, dataset, table_id, destination_uri)
    )

    client = bigquery.Client()
    table_id = "abel-traore49.covid19_italy_amine.table_region_code_N"+str(region_code)
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))

    
