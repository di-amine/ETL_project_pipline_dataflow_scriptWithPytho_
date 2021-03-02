import argparse
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT='abel-traore49'
BUCKET='myb_amine22'




def run(argv=None):

    parser = argparse.ArgumentParser()    

    known_args, pipeline_args = parser.parse_known_args(argv)
    region_code = 1  
    filecsv='tablcovid'+str(region_code)+'.csv'      
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:    


        BQ_DATA = p | 'read_bq_view' >> beam.io.Read(           
                    beam.io.BigQuerySource(query =  'SELECT * FROM `abel-traore49.covid19_italy_amine.covid19it` WHERE region_code = {} ;'.format(region_code), use_standard_sql=True))

        BQ_VALUES = BQ_DATA | 'read values' >> beam.FlatMap(lambda x: x.values())

        CSV_FORMAT = BQ_VALUES  | 'CSV format' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))

        BQ_CSV = CSV_FORMAT  | 'Write_to_GCS' >> beam.io.WriteToText(
            'gs://{0}/results/output'.format(BUCKET), file_name_suffix='.csv', 
            header='date, country, region_code, region_name, latitude, longitude, location_geom, hospitalized_patients_symptoms, hospitalized_patients_intensive_care, total_hospitalized_patients, home_confinement_cases, total_current_confirmed_cases, new_current_confirmed_cases, new_total_confirmed_cases, recovered, deaths, total_confirmed_cases, tests_performed, note')

    p.run().wait_until_finish()

       

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
