# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 11:41:05 2020

@author: micha
"""
import timeit

import pyarrow.parquet as pq
import pandas as pd
import s3fs
import datetime
import itertools

from settings import Settings, QuerySettings

s3 = s3fs.S3FileSystem(anon=False)

def process_read_parquet():
    now = datetime.datetime.now().strftime("%Y%m%dT%H%M")
    parquet_read_timing = pd.DataFrame()
    
    for dataset, sufix in itertools.product(QuerySettings.datasets,
                                            QuerySettings.dataset_sufixes):
    
        start_time = datetime.datetime.now()
        result = pq.read_table("s3://simm-poc-s3-athena/simm/"
                               + f"{str(Settings.use_dict_encoding).lower()}/"
                               + f"{Settings.compression}/"
                               + f"{''.join(Settings.partition_cols)}/"
                               + f"{dataset}_{sufix}", filesystem=s3)
        schema = result.schema
        end_time = datetime.datetime.now()
                
        timing = {"Dataset": dataset,
                  "dataset_sufix": sufix,
                  "rows": result.num_rows,
                  "duration": end_time - start_time}
        
        parquet_read_timing = parquet_read_timing.append(pd.DataFrame(timing, index=[0]), ignore_index=True)
            
    parquet_read_timing.to_csv("C:\\Users\\micha\\Desktop\\SIMM\\output\\"
                                "parquet_read_timing_" + now + ".csv")

if __name__ == "__main__":
    process_read_parquet()
