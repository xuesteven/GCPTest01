#!/usr/bin/env python
import apache_beam as beam
import re


PROJECT='stevenxue20181006'
BUCKET='stevenxue20181006'

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=examplejob2',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner'
   ]

   p = beam.Pipeline(argv=argv)
   input = 'gs://{0}/test01.txt'.format(BUCKET)
   output_prefix = 'gs://{0}/test01/output'.format(BUCKET)

   # find all lines that contain the searchTerm
   (p
      | 'GetFile' >> beam.io.ReadFromText(input)
      | 'write' >> beam.io.WriteToText(output_prefix)
   )

   p.run()

if __name__ == '__main__':
   run()
