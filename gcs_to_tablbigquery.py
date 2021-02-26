
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    def parse_method(self, string_input):
        # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',
                                                         string_input)))
        row = dict(
            zip(('date', 'country', 'region_code', 'region_name', 'latitude', 'longitude', 'location_geom', 'hospitalized_patients_symptoms', 'hospitalized_patients_intensive_care', 'total_hospitalized_patients', 'home_confinement_cases', 'total_current_confirmed_cases', 'new_current_confirmed_cases', 'new_total_confirmed_cases', 'recovered', 'deaths', 'total_confirmed_cases', 'tests_performed', 'note'),
                values))
        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()


    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',

        default='gs://myb_amine22/covid19_italy.csv')


    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='covid19_italy_amine.covid19italytestpy')


    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()


    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p

     | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)

     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s)) 
     | 'Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType
             schema='date:TIMESTAMP,country:STRING,region_code:INTEGER,region_name:STRING,'
             'latitude:FLOAT,longitude:FLOAT,location_geom:STRING,'
             'hospitalized_patients_symptoms:INTEGER,hospitalized_patients_intensive_care:INTEGER,'
             'total_hospitalized_patients:INTEGER,home_confinement_cases:INTEGER,'
             'total_current_confirmed_cases:INTEGER,new_current_confirmed_cases:INTEGER,'
             'new_total_confirmed_cases:INTEGER,recovered:INTEGER,deaths:INTEGER,'
             'total_confirmed_cases:INTEGER,tests_performed:INTEGER,note:STRING',
             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
