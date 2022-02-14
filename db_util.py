from . import json_util
from pymongo import MongoClient
from tqdm import tqdm 


def export_table(*,
                 dbms: str, 
                 host: str,
                 database: str,
                 table: str, 
                 output_path: str,
                 use_tqdm: bool = True):
    """
    导出数据库中的表。
    
    以JSON文件形式导出表的DDL（如果有的话）和所有记录，每一条记录占一行。
    
    支持的数据库(DBMS)如下：
    1. MySQL
    2. MongoDB(table_path: ['MongoDB', ip_addr, database, collection])
    3. Elasticsearch
    """
    assert output_path.endswith('.json')
    
    with open(output_path, 'w', encoding='utf-8') as fp:
        if dbms.lower() == 'MongoDB'.lower():
            mongo_client = MongoClient(host)
            mongo_collection = mongo_client[database][table]
            
            for entry in tqdm(mongo_collection.find(), disable=not use_tqdm):
                json_str = json_util.json_dump(entry).strip()
                print(json_str, file=fp, flush=True)
        else:
            raise AssertionError
