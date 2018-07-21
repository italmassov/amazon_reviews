# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 14:24:47 2017
@author: dblitalmk
"""
# uploading epos ecig data to Database
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:12345@localhost:5432/amazon_reviews')

###########################
# create database
def create_reviews():
    create_query = '''
    CREATE TABLE public.reviews
    (
        "reviewerID" character varying(21) COLLATE pg_catalog."default" NOT NULL,
        asin character varying(10) COLLATE pg_catalog."default" NOT NULL,
        "reviewerName" character varying(50) COLLATE pg_catalog."default",
        helpful1 integer,
        helpful2 integer,
        "helpfulRatio" double precision,
        "reviewText" character varying(35000) COLLATE pg_catalog."default",
        overall double precision,
        summary character varying(512) COLLATE pg_catalog."default",
        "reviewTime" date,
        CONSTRAINT "reviews1" UNIQUE("reviewerID",asin,"reviewTime")
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    
    ALTER TABLE public.reviews
        OWNER to postgres;
    
    '''
    # remove table if exists
    engine.execute('DROP TABLE public.reviews')
    engine.execute(create_query)
        
if __name__ == '__main__':
    create_reviews()