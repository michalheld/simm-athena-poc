# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 12:06:52 2020

@author: micha
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 14:40:26 2020

@author: micha
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime
import s3fs

from random_data_generator import RandomData
from settings import Constants, Settings
from athena import Athena
from read_from_athena import read_process
from read_parquet import process_read_parquet

def write_process(load_id_):
    s3 = s3fs.S3FileSystem(anon=False)
    now = datetime.datetime.now().strftime("%Y%m%dT%H%M")
    
    dataset_schema = RandomData(Constants.INPUT_DATASET_SCHEMA_DICT, load_id_)
    dataset_schema.read_files()

    athena_client = Athena()
    
    parquet_creation_timing = pd.DataFrame()
    
    if Settings.new_parquet:
        dataset_sufix = now.lower()
        create_table = True
    else:
        dataset_sufix = Settings.existing_parquet
        create_table = False
    
    for dataset, atomic_load_count, data in dataset_schema.generate_random_data_list():
        start_time = datetime.datetime.now()
        print(start_time)
        parquet_table = pa.Table.from_pandas(data)   
        s3path = ("s3://simm-poc-s3-athena/simm/"
                 + str(Settings.use_dict_encoding).lower()
                 + "/"
                 + Settings.compression 
                 + "/"
                 + "".join(Settings.partition_cols)
                 + "/"
                 + dataset
                 + "_"
                 + dataset_sufix)
        
        partition_list = []
        for column in Settings.partition_cols:
            partition_list.append((column, data[column].iloc[0]))
        partition_path = "/".join(["=".join(i) for i in partition_list])       
        
        s3partpath = (s3path 
                      + "/"
                      + partition_path)
        if Settings.remove_existing_part and s3.exists(s3partpath):
            s3.rm(s3partpath, recursive=True)
            print("Remove partition: SUCCESS")
        
        print(partition_path)
        print(atomic_load_count)
        
        pq.write_to_dataset(parquet_table, 
                            s3path,
                            filesystem=s3,
                            partition_cols=Settings.partition_cols,
                            coerce_timestamps="ms",
                            allow_truncated_timestamps=True,
                            use_dictionary=Settings.use_dict_encoding,
                            compression=Settings.compression)
        print("Push to S3: SUCCESS")
        
        if create_table:
            athena_client.create_table(dataset + "_" + dataset_sufix, 
                                        partition_list, 
                                        s3path)
            print("Create table: SUCCESS")
            create_table = False
                
        athena_client.refresh_metastore(dataset + "_" + dataset_sufix,
                                        partition_list)
        print("Refresh metastore: SUCCESS")
        
        end_time = datetime.datetime.now()
        creation_timing = {"Dataset": dataset,
                           "dataset_sufix": dataset_sufix,
                           "use_dict_encoding": str(Settings.use_dict_encoding).lower(),
                           "compression": Settings.compression,
                           "partition_cols": str(Settings.partition_cols),
                           "load_id": dataset_schema.load_id[dataset][load_id_] - 1,
                           "Dataset size": atomic_load_count, 
                           "duration": end_time - start_time}
        parquet_creation_timing = parquet_creation_timing.append(pd.DataFrame(creation_timing, index=[0]), ignore_index=True)
        
    parquet_creation_timing.to_csv("C:\\Users\\micha\\Desktop\\SIMM\\output\\"
                                    "parquet_toS3_timing_" + dataset_sufix + "_" + now + ".csv")
  
for load_id_ in range(len(next(iter(Constants.DATASET_LOAD_ID.values())))):
    write_process(load_id_)
    read_process()
    # process_read_parquet()
