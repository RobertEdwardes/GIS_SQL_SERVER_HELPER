import geopandas as gpd
import pyodbc 
def gis_upload(file, table_name, database, conn, crs=None, geo_col='geometry', geo_type='geometry', **schema):
    cxn = conn.cursor()
    if not crs and geo_type == 'geometry':
        raise Exception('Please include CRS or use Geography data type for GIS Feature data')
    if isinstance(geo_col, str) and isinstance(geo_type, list):
        raise Exception('Please include geo_col list that is the same length as geo_type')
    df = gpd.read_file(file)
    df = df.to_crs(crs)
    cols = [i for i in df.columns if i not in geo_col]
    dfTables = pd.read_sql(f""" SELECT TABLE_NAME 
            FROM [{database}].INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' """, conn)
    dfTables = dfTables['TABLE_NAME'].tolist()
    if tableName not in dfTables:
        sql_col_dtype []
        if schema == {}:
            for j in cols:
                av = df[j].str.mean()
                mx = df[j].str.max()
                if av == mx:
                    sql_col_dtype.append(f'{str(elem)} Char({mx})') 
                else:
                    sql_col_dtype.append(f'{str(elem)} Varchar({mx})') 
            if isinstance(geo_col, list):
                geo_lst = zip(geo_col,geo_type)
                for j , kin geo_lst:
                    sql_col_dtype.append(f'{str(j)} {k}') 
            else:
                sql_col_dtype.append(f'{str(geo_col)} {geo_type}')
        tableCols = ','.join(sql_col_dtype)        
        createTableString = f"""
                CREATE TABLE {tableName} (
                    {tableCols}
                );
            """
        cxn.execute(createTableString)
        cxn.commit()
    insertHead = ','.join([str(elem) for elem in cols])G
    for row in df.iterrows():
        valgeo = row[1][geo_col]
        val = row[1].tolist()
        val = val[:-1]
        val = ','.join(["'"+str(elem).replace("'", "''")+"'" for elem in val])
        retry = True
        trys = 0
        while retry:
            try:
                insertSQL = f"insert into {tableName} ({insertHead}) values ({val}, {geo_type}::STGeomFromText('{valgeo}', {crs}))"
                insertSQL = insertSQL.replace('None','NULL')
                cxn.execute(insertSQL)
                cxn.commit()
                retry = False
            except:
                trys += 1
                print(trys)
                time.sleep(60)