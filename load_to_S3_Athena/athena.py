import boto3
import time

from settings import Settings

class Athena:
    
    def __init__(self):
        self.client = boto3.client('athena')
        
    def refresh_metastore(self, dataset_sufix, partition_list):
        partition_list = [(i, j.replace(';', '\;')) for i, j in partition_list]
        partition_path = ", ".join([f"{i[0]}='{i[1]}'" for i in partition_list])       
            
        response = self.client.start_query_execution(
            QueryString=f"ALTER TABLE {dataset_sufix} " 
                        f"ADD IF NOT EXISTS "
                        f"PARTITION({partition_path})",
            QueryExecutionContext={"Database": "simm_poc"},
            ResultConfiguration={"OutputLocation": "s3://simm-poc-s3-athena/athena/query_results/"}                        
            )
        return response
    
    def create_table(self, dataset_sufix, partition_list, s3path):
        partition_string = ", ".join(["`" + i[0] + "` string" for i in partition_list])
        query = (
            f"CREATE EXTERNAL TABLE " 
            f"IF NOT EXISTS {dataset_sufix} ("
            f"`RiskDate` timestamp,"
            f"`RiskType` string,"
            f"`Qualifier` double,"
            f"`Bucket` double,"
            f"`Label1` double,"
            f"`Label2` double,"
            f"`Amount` double,"
            f"`AmountCurrency` string,"
            f"`AmountUSD` double,"
            f"`ProductClass` string,"
            f"`SecDBAssetName` double,"
            f"`VegaVolLevel` double,"
            f"`ETI` string,"
            f"`SecurityType` string,"
            f"`SecurityName` string,"
            f"`LeafQuantity` int,"
            f"`Denominated` string,"
            f"`ExpirationDate` string,"
            f"`SecDbBook` string,"
            f"`SecDbBookId` int,"
            f"`SecDbGroup` string,"
            f"`PositionDate` string,"
            f"`IsClearable` double,"
            f"`IsVanillaXccySwap` int,"
            f"`XccyNotionalExcluded` int,"
            f"`EquityIndexRiskExploded` string,"
            f"`CommodIndexRiskExploded` string,"
            f"`IsRiskCurrent` int,"
            f"`VersionTag` string,"
            f"`PositionSource` string,"
            f"`IsIRBasisIncluded` double,"
            f"`__uploader_id` string,"
            f"`ProcessName` string,"
            f"`TdsWrittenTime` string,"
            f"`dbinserttime` string"
            f") PARTITIONED BY ("
            f"{partition_string})"
            f"STORED AS PARQUET "
            f"LOCATION " 
            f"'{s3path}'"
            f"TBLPROPERTIES ('parquet.compress'='{Settings.compression}')"
            )
        return self._execute_query(query)
    
    def _execute_query(self, query: str):
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "simm_poc"},
            ResultConfiguration={"OutputLocation": "s3://simm-poc-s3-athena/athena/query_results/"}
            )
        return response
    
    def _get_query_status(self, query_id: str):
        result = self.client.get_query_execution(
            QueryExecutionId=query_id
            )
        return result["QueryExecution"]["Status"]["State"]
    
    def _get_query_result(self, query_id: str, limit: int):
        result = self.client.get_query_results(
            QueryExecutionId=query_id,
            MaxResults=limit
            )
        return result
    
    def _get_next_result(self, query_id: str, nexttoken: str, limit: int):
        result = self.client.get_query_results(
            QueryExecutionId=query_id,
            NextToken=nexttoken,
            MaxResults=limit
            )
        return result
    
    def run_query(self, query: str, limit: int = 1000):
        response = self._execute_query(query)
        query_id = response["QueryExecutionId"]
        while self._get_query_status(query_id) in ("QUEUED", "RUNNING"):
            time.sleep(1)
        if self._get_query_status(query_id) in ("FAILED", "CANCELLED"):
            raise Exception("Query failed")
        result = self._get_query_result(query_id, limit)
        next_token = result.get("NextToken")
        yield result
        while next_token:
            result = self._get_next_result(query_id, next_token, limit) 
            next_token = result.get("NextToken")
            yield result