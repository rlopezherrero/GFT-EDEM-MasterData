#ts
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.options.pipeline_options import SetupOptions

import TableSchema
#import FormatQuote
import json


class FormatQuote(beam.DoFn):
   
    
    def process(self, element):
        
        quote = json.loads(element)['quote']
        #return [quote]
        return [{'symbol':quote['symbol'],
                 'companyName':quote['companyName'],
                 'primaryExchange':quote['primaryExchange'],
                 'calculationPrice':quote['calculationPrice'],
                 'open':quote['open'],
                 'openTime':quote['openTime'],
                 'close':quote['close'],
                 'closeTime':quote['closeTime'],
                 'high':quote['high'],
                 'low':quote['low'],
                 'latestPrice':quote['latestPrice'],
                 'latestVolume':quote['latestVolume'],
                 'latestTime':quote['latestTime'],
                 'latestUpdate':quote['latestUpdate'],
                 
                 }]
 
        
    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      #default='projects/YOUR_PROJECT_ID/topics/YOUR_TOPIC',
                      default='projects/versatile-gist-251107/topics/iexCloud',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      #2 Add your project Id and Dataset and table name you created
                      # Example versatile-gist-251107:Stocks.quotes',
                      #default='YOUR_PROJECT_ID:YOUR_DATASET.YOUR_TABLE',
                      default='versatile-gist-251107:Stocks.quotes',
                      help='Table where the quotes are stored')
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      #default='projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPION',
                      default='projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      help='Input Subscription')
  
  
  
    
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  #4 Add your project ID
  #Example versatile-gist-251107
  #google_cloud_options.project = 'YOUR_PROJECT_ID'
  google_cloud_options.project = 'versatile-gist-251107'
  google_cloud_options.job_name = 'myjob'
  #5 Add your created bucket
  #€xample edem-bucket-roberto
  google_cloud_options.staging_location = 'gs://edem-bucket-roberto/binaries'
  google_cloud_options.temp_location = 'gs://edem-bucket-roberto/temp'

  #pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


  p = beam.Pipeline(options=pipeline_options)

  quotesSchema=TableSchema.tableSchema.quotes_schema

  # Read the pubsub messages into a PCollection.
  quotes = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  
  quotesFormatted = ( quotes | 'Format Quote' >> beam.ParDo(FormatQuote()))

  quotesFormatted | 'Print Quote' >> beam.Map(print)
   

  quotesFormatted | 'Store Big Query' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema=quotesSchema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()