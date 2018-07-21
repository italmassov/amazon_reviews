# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 14:08:07 2018

@author: DBLITALMK

Data processing utils
"""
import os
import sys

import tarfile
import gzip

from random import random
import datetime
import pandas as pd
import numpy as np

dir_path = os.path.dirname(os.path.realpath(__file__))

def maybe_extract(filename, force=False):
  root = os.path.splitext(os.path.splitext(filename)[0])[0]  # remove .tar.gz
  if os.path.isdir(root) and not force:
    # You may override by setting force=True.
    print('%s already present - Skipping extraction of %s.' % (root, filename))
  else:
    print('Extracting data for %s. This may take a while. Please wait.' % root)
    tar = tarfile.open(filename)
    sys.stdout.flush()
    tar.extractall(dir_path)
    tar.close()

  data_folders = [ os.path.join(root, d) for d in sorted(os.listdir(root))
                      if os.path.isdir(os.path.join(root, d))]
  print(data_folders)
  return data_folders

def getDF(path):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
  return pd.DataFrame.from_dict(df, orient='index')

# sample versions
def parseSample(path, fraction):
  g = gzip.open(path, 'rb')
  for l in g:
      if random() <= fraction:
          yield eval(l)
          
def getDFSample(path, fraction):
  i = 0
  df = {}
  for d in parseSample(path, fraction):
    df[i] = d
    i += 1
  return pd.DataFrame.from_dict(df, orient='index')

# partial uploads
def getDFTop(path, top=100):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1    
    if i == top:
        return pd.DataFrame.from_dict(df, orient='index')
  
def parse(path, start_row=0):
  j = 0
  g = gzip.open(path, 'rb')
  for l in g:
    j += 1
    if j >= start_row:
        yield eval(l)

def getDFStream(path, splits=50000, start_iteration=0):
  df = {}
  i = 0
  for d in parse(path,start_iteration*splits):
      df[i] = d
      i += 1
      if i % splits == 0 and i !=0:
          i = 0
          yield pd.DataFrame.from_dict(df, orient='index')          

# process df for database
def process_review_time(StrTime, UnixTime):
    try:
        review_time = pd.datetime.strptime(StrTime,"%m %d, %Y")
    except:
        try:
            review_time = datetime.datetime.fromtimestamp(UnixTime)
        except:
            review_time = np.nan
            
    return review_time # pd.datetime.strftime(review_time, '%Y-%m-%d')
        
def process_reviews(df):
    df2 = df.copy()
    df2['helpful1'] = df2['helpful'].apply(lambda x: x[0])
    df2['helpful2'] = df2['helpful'].apply(lambda x: x[1])
    df2['helpfulRatio'] = df2['helpful'].apply(lambda x: x[0]/x[1] if x[1]>0 else 0)
    
    df2['reviewTime'] = df2[['reviewTime', 'unixReviewTime']].apply(lambda x: process_review_time(x[0], x[1]), axis=1)
    
    df2['reviewText'] = df2['reviewText'].apply(lambda x: x[:35000] if isinstance(x,str) else np.nan)

    df2['summary'] = df2['summary'].apply(lambda x: x[:512] if isinstance(x,str) else np.nan)
    
    # removing '\x00' from strings
#    df2['reviewerName'] = df2['reviewerName'].replace('\x00', '')
#    df2['summary'] = df2['summary'].replace('\x00', '')
    df2['reviewText'] = df2['reviewText'].apply(lambda x: x.replace('\x00', '') if isinstance(x,str) else x)
    
    return df2[['reviewerID','asin','reviewerName','helpful1','helpful2','helpfulRatio','reviewText','overall','summary','reviewTime']]


# process meta for database
def process_meta(df):
    df2 = df.copy()
    df2['categories'] = df2['categories'].apply(lambda x: '|'.join(x[0]) if type(x) == list and pd.notnull(x[0]).all() else np.nan)
    df2['salesRank'] = df2['salesRank'].apply(lambda x: '|'.join([ "%s:%s" % (k,v) for k,v in x.items()]) if type(x) == dict and pd.notnull(x) else np.nan)
    
    df2['also_bought'] = df2['related'].apply(lambda x: ','.join(x['also_bought']) if type(x) == dict and pd.notnull(x) and 'also_bought' in x else np.nan)
    df2['also_viewed'] = df2['related'].apply(lambda x: ','.join(x['also_viewed']) if type(x) == dict and pd.notnull(x) and 'also_viewed' in x else np.nan)
    df2['bought_together'] = df2['related'].apply(lambda x: ','.join(x['bought_together']) if type(x) == dict and pd.notnull(x) and 'bought_together' in x else np.nan)

    df2['description'] = df2['description'].apply(lambda x: x[:1000] if isinstance(x,str) else np.nan)
    df2['title'] = df2['title'].apply(lambda x: x[:1500] if isinstance(x,str) else np.nan)
    df2['brand'] = df2['brand'].apply(lambda x: x[:1000] if isinstance(x,str) else np.nan)
   
    return df2[['asin','categories','salesRank','title','description','price','also_bought','also_viewed','bought_together','brand']]


##############################################################################
# text version of files
def parseText(path):
  f = open(path, 'rb')
  for i, l in enumerate(f):
    yield eval(l)
    
def getTextDFStream(path, splits=50000):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
    if i % splits == 0 and i !=0:
        yield pd.DataFrame.from_dict(df, orient='index')

##############################################################################    
# Filtered versions
def parseFilter(path, category='Books'):
  g = gzip.open(path, 'rb')
  for l in g:
      if category in l:
          yield eval(l)

def getDFStreamFilter(path, category, splits=50000):
  i = 0
  df = {}
  for d in parseFilter(path, category):
    df[i] = d
    i += 1    
    if i % splits == 0 and i !=0:
        yield pd.DataFrame.from_dict(df, orient='index')