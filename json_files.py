import pyodbc 
import geopandas
import pandas
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import os


def create_json_table(table_name, cursor, idx_key=None, col_store=None):
    if not col_store and not idx_key:
        sql_query_basic=f"""create table {table_name} (
                _id bigint primary key identity,
                dict nvarchar(max)
        );
        ALTER TABLE {table_name}
        ADD CONSTRAINT [Log record should be formatted as JSON]
                    CHECK (ISJSON(dict)=1);"""
        cursor.excute(sql_query)
        cursor.commit()
    elif col_store and not idx_key:
        sql_query_colStore=f"""create sequence {table_name}ID as bigint;
                    go
                    create table {table_name} (
                        _id bigint default(next value for {table_name}ID),
                        dict nvarchar(max),

                        INDEX cci CLUSTERED COLUMNSTORE
                    );"""
        cursor.excute(sql_query_colStore)
        cursor.commit()
    elif idx_key and not col_store:
        sql_query_idx = f"""create table {table_name} (
            _id bigint primary key identity,
            log nvarchar(max),

            severity AS JSON_VALUE(log, '$.{idx_key}'),
            index ix_severity ({idx_key})
        );"""
        cursor.excute(sql_query_idx)
        cursor.commit()

