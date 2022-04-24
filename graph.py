import pyodbc 
import geopandas
import pandas
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import os

def create_node_table(node_name, schema):
    sql_query = f"""
    CREATE TABLE {node_name} (
            ID INTEGER PRIMARY KEY, 
            {schema}
    ) AS NODE;
    """

def create_edge_table(edge_name, schema):
    sql_query = f"""
    CREATE TABLE {edge_name} (
        id INTEGER PRIMARY KEY,
        {schema}
    ) AS EDGE;
    """
def insert_edge(filer_value, node_a, node_b, edge_name, node_name, value):
    sql_query = f"""INSERT INTO {edge_name} VALUES ((SELECT $node_id FROM {node_name} WHERE {filer_value} = '{node_a}'),
        (SELECT $node_id FROM {node_name} WHERE {filer_value} = '{node_b}'), {value});"""