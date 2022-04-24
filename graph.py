import pyodbc 
import geopandas
import pandas
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import os

def create_node_table(node_name, schema, idx_name='ID'):
    sql_query = f"""
    CREATE TABLE {node_name} (
            {idx_name} INTEGER PRIMARY KEY, 
            {schema}
    ) AS NODE;
    """

def create_edge_table(edge_name, schema, idx_name='id'):
    sql_query = f"""
    CREATE TABLE {edge_name} (
        {idx_name} INTEGER PRIMARY KEY,
        {schema}
    ) AS EDGE;
    """
    
def insert_edge(node_a, node_b, edge_name, node_table, value, filer_value='ID'):
    sql_query = f"""INSERT INTO {edge_name} VALUES ((SELECT $node_id FROM {node_table} WHERE {filer_value} = '{node_a}'),
        (SELECT $node_id FROM {node_table} WHERE {filer_value} = '{node_b}'), {value});"""


def df_to_NODEDGE(df, node_id, edge_id, *edge_values, *node_values):
    nv = node_values.append(node_id)
    NODE = df[nv]
    ev = edge_values.append(edge_id)
    EDGE = df[ev]
    NODE = NODE.to_json(orient="records")
    EDGE = EDGE.to_json(orient="records")
    