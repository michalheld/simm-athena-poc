# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 16:43:12 2020

@author: micha
"""
import pandas as pd
import itertools
import datetime

from athena import Athena
from settings import QuerySettings

def read_process():
    now = datetime.datetime.now().strftime("%Y%m%dT%H%M")
    athena_query_timing = pd.DataFrame()
    athena_client = Athena()
    
    for dataset, sufix, query in itertools.product(QuerySettings.datasets,
                                                   QuerySettings.dataset_sufixes,
                                                   QuerySettings.query_pattern):
        start_time = datetime.datetime.now()
        table = f'"simm_poc"."{dataset}_{sufix}"'
        query_string = query.replace("{}", table)
        response = athena_client.run_query(query_string)
        result = next(response)
        end_time = datetime.datetime.now()
        
        timing = {"Dataset": dataset,
                  "dataset_sufix": sufix,
                  "query": query_string,
                  "example_result": str(result.get("ResultSet").get("Rows")[1].get("Data")),
                  "duration": end_time - start_time}
        athena_query_timing = athena_query_timing.append(pd.DataFrame(timing, index=[0]), ignore_index=True)
            
    athena_query_timing.to_csv("C:\\Users\\micha\\Desktop\\SIMM\\output\\"
                                "athena_query_timing_" + now + ".csv")
    
if __name__ == "__main__":
    read_process()
