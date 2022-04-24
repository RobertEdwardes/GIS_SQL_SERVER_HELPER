import pyodbc 
import geopandas
import pandas
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import os

def create_creditals(credential_name, azure_blob_seceret, cursor):
    try:
        sql_query=f"""REATE DATABASE SCOPED CREDENTIAL {credential_name}
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = '{azure_blob_seceret}';"""
        cursor.excute(sql_query)
        cursor.commit()
    except Exception as e:
        print(e)

def create_external_data_source(credential_name ,azure_url, source_name, cursor, source_type='BLOB_STORAGE'):
    try:
        sql_query = f"""CREATE EXTERNAL DATA SOURCE {source_name}
        WITH (
            TYPE = {source_type},
            LOCATION = '{azure_url}',
            CREDENTIAL = {credential_name}
        );"""
        cursor.excute(sql_query)
        cursor.commit()
    except Exception as e:
        print(e)

def create_connection(server, database, username, password, driver='SQL Server'):
    try:
        cnxn = pyodbc.connect('DRIVER={'+driver+'};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        cursor.execute("SELECT @@version;") 
        cursor.commit
        return cursor
    except Exception as e:
        print(e)

def azure_connect():
    try:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    except Exception as e:
        print(e)
        
def upload_azure_blob(file_name, container_name, local_path=None):
    upload_file_path = os.path.join(local_path, file_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data)