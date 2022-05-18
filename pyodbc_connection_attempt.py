import itertools
import readconfig
import pandas as pd
import os
import pyodbc
import tqdm

# Set the working directory to the location of scripts
path = "C:\\Users\\Nitro\\Desktop\\BI group project\\BI_final_project"
os.chdir(path)

# Call to read the configuration file
c_ER = readconfig.get_sql_config(r'SQL_Server_Config.cfg', "Database1")

# Create a connection string for SQL Server
conn_info_ER = 'Driver={};Server={};Database={};Trusted_Connection={};'.format(*c_ER)

# Connect to the server and to the desired database
conn_ER = pyodbc.connect(conn_info_ER)

# Create a Cursor class instance for executing T-SQL statements
cursor_ER = conn_ER.cursor()


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
    table_primary_key = cursor.primaryKeys(table_name, schema=schema)

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


def populate_ER(db='proj2_db', src='data_source.xlsx'):
    """
    Populate ER table
    """
    cursor_ER.execute(f'use {db}')
    cursor_ER.execute(f'ALTER TABLE Employees DROP CONSTRAINT reportsto;')
    for sheet in ['Employees', 'Region', 'Territories', 'Suppliers', 'Categories', 'Products', 'EmployeeTerritories',
                  'Customers', 'Shippers', 'Orders', 'OrderDetails']:
        sheet_data = pd.read_excel(src, sheet_name=sheet)
        for _, row in sheet_data.iterrows():
            columns = row.index.to_list()
            row_data = [f"""'{str(col).replace("'", "''")}'""" if (
            (not isinstance(col, (float, int)) or (str(col) in ['True', 'False']))) else f"{col}" for col in
                        row.to_list()]
            row_data = ['null' if (col in ['nan']) else col for col in row_data]
            row_data = ['null' if (col in ["'NaT'"]) else col for col in row_data]
            cmd = f'insert into dbo.{sheet}({",".join(columns)}) values ({", ".join(row_data)})'
            print(cmd)
            cursor_ER.execute(cmd)
    cursor_ER.commit()
    cursor_ER.execute("""
        ALTER TABLE Employees
        ADD CONSTRAINT reportsto 
        FOREIGN KEY (ReportsTo)
        REFERENCES Employees(EmployeeID);
        """)


if __name__ == '__main__':
    populate_ER()
    cursor_ER.close()
    conn_ER.close()
