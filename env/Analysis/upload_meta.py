# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 23:25:32 2018

@author: DBLITALMK
Upload reviews File to Database
"""
import os
os.chdir('C:/Users/DBLITALMK/Documents/Legacy/Case Study/env/Analysis/')

from utils import getDFSample, process_meta, getDFStream,getDFTop,getDFStreamFilter
from sqlalchemy import create_engine

print('connecting to database')
engine = create_engine('postgresql://postgres:12345@localhost:5432/amazon_reviews')

# test process reviews date ##############
index = 0
for df in getDFStream('metadata.json.gz', splits=200000):
    index += 1
    print('iteration %d' % index)
    t = process_meta(df)
    print('   data retrieved')
    print('   description %d' % max( t['description'].apply(lambda x: len(x) if isinstance(x,str) else 0) ) )
    t.to_sql('meta',engine, if_exists='append', index=False)
    print('   data uploaded')