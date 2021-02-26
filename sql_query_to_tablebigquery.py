import argparse
import logging
import re
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT = 'abel-traore49'
BUCKET = 'myb_amine22'
dataset='covid19_italy_amine'
table_id='covid19it'


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()
    pipeline_args = parser.parse_known_args(argv)
    
    for region_code in range(1, 20):
        filecsv = 'tablcovid'+str(region_code)+'.csv'
        p = beam.Pipeline(options=PipelineOptions(pipeline_args))

        (p


        # Des requets SQL pour préparé a la creation des tables pour chaque ville
        
         | 'read_bq_view' >> beam.io.Read(

             beam.io.BigQuerySource(query='SELECT * FROM `{}.{}.{}` WHERE region_code = {} ;'.format(PROJECT, dataset, table_id, region_code), use_standard_sql=True))
         

        # Création les table pour chaque région à partir des requet SQL

         | 'Write_bq_table' >> beam.io.WriteToBigQuery(
             table='tablcovid'+str(region_code),
             project='abel-traore49',
             schema='date:TIMESTAMP,country:STRING,region_code:INTEGER,region_name:STRING,'
             'latitude:FLOAT,longitude:FLOAT,location_geom:STRING,'
             'hospitalized_patients_symptoms:INTEGER,hospitalized_patients_intensive_care:INTEGER,'
             'total_hospitalized_patients:INTEGER,home_confinement_cases:INTEGER,'
             'total_current_confirmed_cases:INTEGER,new_current_confirmed_cases:INTEGER,'
             'new_total_confirmed_cases:INTEGER,recovered:INTEGER,deaths:INTEGER,'
             'total_confirmed_cases:INTEGER,tests_performed:INTEGER,note:STRING',
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
        p.run().wait_until_finish()



#********************************************************************************
        # Extraire les données de Bigquery vers GCS au format CSV.

#********************************************************************************

        client = bigquery.Client()
        dataset_id = "covid19_italy_amine"
        table = 'tablcovid'+str(region_code),

        destination_uri = "gs://{}/imported_CSV_files/{}".format(
            BUCKET, filecsv)
        dataset_ref = bigquery.DatasetReference(PROJECT, dataset_id)
        table_ref = dataset_ref.table(table)

        extract_job = client.extract_table(table_ref, destination_uri,
                                           # Location must match that of the source table.
                                           location="US",
                                           )  # API request
        extract_job.result()  # Waits for job to complete.

        print(
            "Exported {}:{}.{} to {}".format(
                PROJECT, dataset_id, table, destination_uri)
        )



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
