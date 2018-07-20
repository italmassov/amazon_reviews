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
    
def create_meta():
    create_query = '''
    CREATE TABLE public.meta
    (
        asin character varying(10) COLLATE pg_catalog."default" NOT NULL,
        categories character varying(200) COLLATE pg_catalog."default",
        "salesRank" character varying(40) COLLATE pg_catalog."default",
        "title" character varying(1500) COLLATE pg_catalog."default",
        "description" character varying(1000) COLLATE pg_catalog."default",
        price double precision,
        "also_bought" character varying(1200) COLLATE pg_catalog."default",
        "also_viewed" character varying(1300) COLLATE pg_catalog."default",
        "bought_together" character varying(1400) COLLATE pg_catalog."default",
        "brand" character varying(1000) COLLATE pg_catalog."default"
        --CONSTRAINT "meta1" UNIQUE(asin)
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    
    ALTER TABLE public.meta
        OWNER to postgres;
    
    '''
    # remove table if exists
    engine.execute('DROP TABLE public.meta')
    engine.execute(create_query)    
    
    
if __name__ == '__main__':
    create_reviews()