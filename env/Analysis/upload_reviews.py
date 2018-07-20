# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 23:25:32 2018

@author: DBLITALMK
Upload reviews File to Database
"""
import os
os.chdir('C:/Users/DBLITALMK/Documents/Legacy/Case Study/env/Analysis/')

from utils import getDFSample, process_reviews, getDFStream,getDFTop,getDFStreamFilter
from sqlalchemy import create_engine

print('connecting to database')
engine = create_engine('postgresql://postgres:12345@localhost:5432/amazon_reviews')

# test process reviews date ##############
index = 0
for df in getDFStream('item_dedup.json.gz', splits=200000, start_iteration=index):
    print('iteration %d' % index)
    t = process_reviews(df)
    print('   data retrieved')
    t.to_sql('reviews',engine, if_exists='append', index=False)
    print('   data uploaded')
    index += 1
