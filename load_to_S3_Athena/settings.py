# -*- coding: utf-8 -*-
"""
Created on Mon Jul 20 15:05:46 2020

@author: micha
"""

class QuerySettings:
    datasets = ["simm_risk_data_eqderiv"]
    dataset_sufixes = ["20200723t2010"]
    query_pattern = [
        "SELECT COUNT(*) FROM {} WHERE tabledate='20200417'",
        "SELECT risktype, amount FROM {} WHERE amount>10000 AND tabledate='20200417'",
        "SELECT dbinserttime, SUM(amount) FROM {} WHERE tabledate='20200417' GROUP BY dbinserttime",
        "SELECT distinct(amount) FROM {} WHERE tabledate='20200417'",
        "SELECT amount, count(*) count FROM {} WHERE tabledate='20200417' GROUP BY amount",
        "SELECT amount, count(*) count FROM {} WHERE tabledate='20200417' GROUP BY amount HAVING count(*)>1",
        
        # "SELECT * FROM {}",
        "SELECT COUNT(*) FROM {}",
        # "SELECT * FROM {} WHERE amount>10000",
        "SELECT risktype, amount FROM {} WHERE amount>10000",
        "SELECT dbinserttime, SUM(amount) FROM {} GROUP BY dbinserttime",
        "SELECT distinct(amount) FROM {}",
        "SELECT amount, count(*) count FROM {} GROUP BY amount",
        "SELECT amount, count(*) count FROM {} GROUP BY amount HAVING count(*)>1"
        ]

class Settings:
    new_parquet = False
    existing_parquet = "20200723t2010"
    remove_existing_part = False
    use_dict_encoding = True #True, False
    compression="snappy" #snappy, gzip, brotli, none
    partition_cols=["tabledate", "atomicload"]
    
class Constants:
    INPUT_DATASET_SCHEMA_DICT = "C:\\Users\\micha\\Desktop\\SIMM\\input\\simm_risk_data_eqderiv.txt"
    
    DATASET_RANDOM_INPUT = {
        "otc_trades_legace": {"count": 7,
                              "mean": 350041, 
                              "std": 160516, 
                              "outlier": {20000: 1}},
        "simm_risk_data_eqderiv": {"count": 1394,#3394,
                                   "mean": 51,
                                   "std": 105,
                                   "outlier": { 
                                                # 1043083: 1
                                                # 10430837: 1
                                                700: 4,
                                                800: 10,
                                                900: 17,
                                                1000: 48,
                                                2000: 30,
                                                3000: 16,
                                                4000: 12,
                                                5000: 3,
                                                6000: 4,
                                                7000: 3,
                                                8000: 1,
                                                10000: 11,
                                                20000: 4,
                                                30000: 3,
                                                40000: 2,
                                                50000: 4,
                                                70000: 3,
                                                90000: 1,
                                                
                                                # 100000: 3,
                                                # 200000: 2,
                                                # 500000: 3,
                                                # 1000000: 3,
                                                # 2000000: 2
                                               }}
    }
    
    DATASET_LOAD_ID = {
        "otc_trades_legace": list(range(180_000, 320_000, 10_000)),
        "simm_risk_data_eqderiv": list(range(180_000, 320_000, 10_000))
    }