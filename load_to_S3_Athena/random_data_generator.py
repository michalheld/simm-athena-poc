# -*- coding: utf-8 -*-
"""
Created on Thu Jul 16 12:35:44 2020

@author: micha
"""

from random import seed, expovariate, randint
from pathlib import Path
import pandas as pd
import datetime
from typing import Dict

from settings import Constants


class FileList(set):
    def all_files(self, source_dir: str):
        path = Path(source_dir)
        if path.is_file():
            self.add(path)
        if path.is_dir():
            for item in path.iterdir():
                self.all_files(str(item))
        return self


class RandomData:     
    dataset_random_output = {}

    def __init__(self, path, load_id):
        self._random_atomic_loads()
        self.path = path
        self.data_from_files: Dict = {}
        self.load_id: Dict = Constants.DATASET_LOAD_ID
        self.load_id_: int = load_id
        
    @classmethod
    def _random_atomic_loads(cls):
        #generating __atomic_load sizes
        seed(1)
        for ds in Constants.DATASET_RANDOM_INPUT:
            input_ = Constants.DATASET_RANDOM_INPUT[ds]
            output_ = []
            for _ in range(input_["count"]):
                random_int = int(expovariate(1/input_["mean"])) + 1
                output_.append(random_int)
            
            for item, repeat in input_["outlier"].items():
                output_ += [item] * repeat
                
            cls.dataset_random_output[ds] = output_

    def read_files(self):
        files = FileList().all_files(self.path)
        for file in files:
            self.data_from_files[file.stem] = reformater[file.stem](pd.read_csv(file))
    
    def _generate_random_data(self, dataset, dataframe, replication):
        seed(1)
        
        dataframe_result = dataframe.loc[dataframe.index.repeat(replication)].reset_index(drop=True)
        dataframe_result = postformater[dataset](dataframe_result)
        
        dataframe_result["DBInsertTime"] = datetime.datetime.now().strftime("%Y%m%dT%H%M%S.%fZ")
        dataframe_result["__atomic_load"] = "atomicload" + str(self.load_id[dataset][self.load_id_])
        self.load_id[dataset][self.load_id_] += 1
        
        # change to lowercase partition columns
        dataframe_result.rename(columns={"TableDate": "tabledate",
                                         "__atomic_load": "atomicload",
                                         "DBInsertTime": "dbinserttime"}, 
                                inplace=True)
        dataframe_result["tabledate"] = dataframe_result["tabledate"].str.lower()
        dataframe_result["atomicload"] = dataframe_result["atomicload"].str.lower()
        dataframe_result["dbinserttime"] = dataframe_result["dbinserttime"].str.lower()
        
        return dataframe_result
        
        
    def generate_random_data_list(self):
        for dataset, dataframe in self.data_from_files.items():
            for atomic_load_count in self.dataset_random_output[dataset]:
                yield (dataset,
                       atomic_load_count, 
                       self._generate_random_data(dataset, dataframe, atomic_load_count))
    
                
class Reformaters:
    
    @staticmethod
    def _reformat_risk(dataframe):
        dataframe["RiskDate"] = pd.to_datetime(dataframe["RiskDate"], infer_datetime_format=True)
        dataframe["TableDate"] = dataframe["RiskDate"].apply(lambda x: x.strftime("%Y%m%d"))
        dataframe["ProductClass"] = "overwrite"
        return dataframe
    
    def _postformat_risk(dataframe):
        dataframe["Amount"] = [randint(0, 100000) + 0.12 for i in range(len(dataframe.index))]
        return dataframe
    
    
reformater = {"simm_risk_data_eqderiv": Reformaters._reformat_risk}
postformater = {"simm_risk_data_eqderiv": Reformaters._postformat_risk}