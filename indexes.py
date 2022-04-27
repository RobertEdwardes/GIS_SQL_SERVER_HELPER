import pyodbc 

## sql_server_extentions template 
## {unquie: , includes: , filter_clase: , data_compression:  , drop_existing: }

def index(idx_type='NONCLUSTERED', idx_name, table_name, *column_name, **sql_server_extentions):
    sql = f'''CREATE {idx_type} INDEX {idx_name}   
            ON {table_name} ({" ".join(column_name)}); '''