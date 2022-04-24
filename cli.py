import click
import os
from tabulate import tabulate
import pandas as pd
import geopandas as gpd
import json

@click.command()
@click.option('--process', '-t', prompt='Choice Process', type=click.Choice(['Create Table','Insert Data','Azure Connection','Index Table']))
@click.option('--path',prompt='Path to working Directory', default=os.getcwd(), type=click.Path(exists=True))
@click.option('--credtials',prompt='Path to SQL Credtials JSON', default=None, type=click.Path(exists=True))

def main(process, path, credtials):
    if not credtials:
        username = click.prompt()
        password = click.prompt()
        database = click.prompt()
        table = click.prompt()
    else:
        if credtials.split(".")[-1] == 'json':
            df = pd.read_json(credtials)
            result = df.to_json(orient="records")
            parsed = json.loads(result)
            username = parsed['username']
            password = parsed['password']
            database = parsed['database']
            table = parsed['table']
        else: 
            username = click.prompt()
            password = click.prompt()
            database = click.prompt()
            table = click.prompt()
    cursor = create_connection(server, database, username, password)
    if process == 'Azure Connection':
        credential_name = click.prompt()
        azure_blob_seceret = click.prompt()
        azure_url = click.prompt()
        source_name = click.prompt()
        create_creditals(credential_name, azure_blob_seceret, cursor)
        create_external_data_source(credential_name ,azure_url, source_name, cursor)
    