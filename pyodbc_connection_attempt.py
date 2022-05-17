# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 08:54:51 2021

@author: arman
"""
#pip install pyodbc
# Import the necessary modules
# Import the config module
import readconfig
import pandas as pd
import os
import pyodbc

# Set the working directory to the location of scripts
os.chdir("C:\AUA\Business Intelligence\Group Project 2")

# Call to read the configuration file
c_ER = readconfig.get_sql_config(r'SQL_Server_Config.cfg',"Database1")

# Create a connection string for SQL Server
conn_info_ER = 'Driver={};Server={};Database={};Trusted_Connection={};'.format(*c_ER)

# Connect to the server and to the desired database
conn_ER = pyodbc.connect(conn_info_ER)

# Create a Cursor class instance for executing T-SQL statements
cursor_ER = conn_ER.cursor()
#%% Auxiliary functions

# Extract the tables names of the database (excluding system tables)    
def extract_tables_db(cursor, *args):
    results = []
    for x in cursor.execute('exec sp_tables'):
        if x[1] not in args:
            results.append(x[2])
    return results

# Extract the column names of a table given it's name
def extract_table_cols(cursor, table_name):
    result = []
    for row in cursor.columns(table=table_name):
        result.append(row.column_name)
    return result

# A function for finding the primary key of a table
def find_primary_key(cursor, table_name, schema):
    
    # Find the primary key information
    table_primary_key =  cursor.primaryKeys(table_name, schema=schema)
    
    # Store the info about the PK constraint into a dictionary
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    try:
        return results[0]
    except:
        pass
    return results


def populate_ER(db = 'proj2_db',src='data_source.xlsx'):
    """
    Populate ER table
    """
    cursor_ER.execute(f'use {db}')
    data = pd.ExcelFile(src)
    for sheet in data.sheet_names:
        sheet_data = pd.read_excel(src,sheet_name=sheet)
        for _,row in sheet_data.iterrows():
            row_data = [f"'{str(col)}'" for col in row.to_list()]
            cmd = f'insert into dbo.{sheet} values ({", ".join(row_data)})'
            print(cmd)
            cursor_ER.execute(cmd)
    cursor_ER.commit()
# populate_ER()
cursor_ER.execute("SELECT * FROM sys.tables")
print(cursor_ER.fetchall())
# for row in cursor_ER.fetchall():
# cursor_ER.execute("use dbo")
# print(extract_tables_db(cursor_ER))
# cursor_ER.execute('select * from Orders')
#
#
#     # Dropping the proc if it exists and commiting the change
#     cursor_dst.execute(sql_script_drop_proc)
#     cursor_dst.commit()
#
#     return f'a
#
# #%% Testing the function
# populate_dim_scd1(cursor_ER,cursor_DW, src_db = 'Orders_ER', src_table = 'brands',
#                   dst_db = 'Orders_DW', dst_table = 'dim_production_brands_SCD1',
#                   src_schema = 'production',dst_schema = 'dbo',
#                   connect_col = 'brand_id')
#
# #%% Close the cursors and the connections
# cursor_ER.close()
# conn_ER.close()
