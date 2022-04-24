import pyodbc 
import geopandas
import pandas
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import os

def insert_flatfile_azure(file_name, source_name, table_name, cursor, file_format='CSV', in_type='bulk'):
    if in_type.lower() = 'bulk'
        try:
            sql_query = f"""BULK INSERT {table_name}
                FROM '{file_name}'
                WITH (DATA_SOURCE = '{source_name}',
                    FORMAT = '{file_format}');"""
            cursor.excute(sql_query)
            cursor.commit()
        except Exception as e:
            print(e)
    elif in_type.lower() = 'openrow':
        try:
            sql_query = f"""INSERT INTO {table_name} with (TABLOCK) (id, description)
                        SELECT * FROM OPENROWSET(
                        BULK  '{file_name}',
                        DATA_SOURCE = '{source_name}',
                        FORMAT ='{file_format}'
                            ) AS DataFile;"""
            cursor.excute(sql_query)
            cursor.commit()
        except Exception as e:
            print(e)