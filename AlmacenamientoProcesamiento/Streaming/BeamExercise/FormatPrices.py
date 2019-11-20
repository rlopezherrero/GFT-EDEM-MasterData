#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 15:39:03 2019

@author: edem
"""

import json 
import apache_beam as beam

class FormatPrices(beam.DoFn):
    """
    Filter data for inserts
    """

    def process(self, element):
        
        price = json.loads(element)
        return [price]
      