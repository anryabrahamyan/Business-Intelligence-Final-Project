import readconfig
import pandas as pd
import os
import pyodbc

path = "C:\\Users\\Nitro\\Desktop\\BI group project\\BI_final_project"
os.chdir(path)


def extract_tables_db(cursor, *args):
    """
    Extract the tables names of the database (excluding system tables)
    """
    results = []
    for x in cursor.execute('exec sp_tables'):
        if x[1] not in args:
            results.append(x[2])
    return results


def extract_table_cols(cursor, table_name):
    """
    Extract the column names of a table given it's name
    """
    result = []
    for row in cursor.columns(table=table_name):
        result.append(row.column_name)
    return result


def find_primary_key(cursor, table_name, schema):
    """
    Find the primary key information
    """
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


def populate_dim_shippers_etl(db='Orders_DIMENSIONAL_DW'):
    """
    Populate The Dim Shippers table from the Relational_Database
    """
    # Call to read the configuration file
    c_ER = readconfig.get_sql_config(r'SQL_Server_Config.cfg', "Database2")
    # Create a connection string for SQL Server
    conn_info_ER = 'Driver={};Server={};Database={};Trusted_Connection={};'.format(*c_ER)
    # Connect to the server and to the desired database
    conn_ER = pyodbc.connect(conn_info_ER)
    # Create a Cursor class instance for executing T-SQL statements
    cursor_ER = conn_ER.cursor()

    cursor_ER.execute(f"use {db}")
    cursor_ER.execute(f'DROP PROCEDURE IF EXISTS dbo.DimShippers_ETL;')
    cursor_ER.execute(
        """
        CREATE PROCEDURE dbo.DimShippers_ETL
        AS
        BEGIN TRY
        MERGE Orders_DIMENSIONAL_DW.dbo.DimShippers AS DST -- destination
        USING proj2_db.dbo.Shippers AS SRC -- source
        ON ( SRC.ShipperID = DST.ShipperID )
        WHEN NOT MATCHED THEN -- there are IDs in the source table that are not in the destination table
          INSERT (ShipperID,
                  CompanyName,
                  Phone)
          VALUES (SRC.ShipperID,
                  SRC.CompanyName,
                  SRC.Phone)
        WHEN MATCHED AND (  -- Isnull - a function that return a specified expression  when encountering null values 
          --Isnull(DST.clientname, '') if DST.clientname == NULL then it will return ''
          Isnull(DST.ShipperID, '') <> Isnull(SRC.ShipperID, '') OR
          Isnull(DST.CompanyName, '') <> Isnull(SRC.CompanyName, '') OR 
          Isnull(DST.Phone, '') <> Isnull(SRC.Phone, '') ) 
          THEN
            UPDATE SET DST.ShipperID = SRC.ShipperID,
                     DST.CompanyName = SRC.CompanyName,
                     DST.Phone = SRC.Phone 
        WHEN NOT MATCHED BY Source THEN
            DELETE;
        END TRY
        BEGIN CATCH
            THROW
        END CATCH
        """
    )
    cursor_ER.execute(f'dbo.DimShippers_ETL;')

    cursor_ER.commit()
    cursor_ER.close()
    conn_ER.close()


def populate_dim_region_etl(db='Orders_DIMENSIONAL_DW'):
    """
    Populate The Dim Region table from the Relational_Database
    """
    # Call to read the configuration file
    c_ER = readconfig.get_sql_config(r'SQL_Server_Config.cfg', "Database2")
    # Create a connection string for SQL Server
    conn_info_ER = 'Driver={};Server={};Database={};Trusted_Connection={};'.format(*c_ER)
    # Connect to the server and to the desired database
    conn_ER = pyodbc.connect(conn_info_ER)
    # Create a Cursor class instance for executing T-SQL statements
    cursor_ER = conn_ER.cursor()

    cursor_ER.execute(f"use {db}")
    cursor_ER.execute(f"DROP PROCEDURE IF EXISTS dbo.DimRegion_ETL;")
    cursor_ER.execute(
        f"""
    CREATE PROCEDURE dbo.DimRegion_ETL
    AS
    -- Define the dates used in validity - assume whole 24 hour cycles
    DECLARE @Yesterday INT =  
    (
       YEAR(DATEADD(dd, - 1, GETDATE())) * 10000
    )
    + (MONTH(DATEADD(dd, - 1, GETDATE())) * 100) + DAY(DATEADD(dd, - 1, GETDATE())) 
    DECLARE @Today INT = --20210413 = 2021 * 10000 + 4 * 100 + 13
    (
       YEAR(GETDATE()) * 10000
    )
    + (MONTH(GETDATE()) * 100) + DAY(GETDATE()) -- Outer insert - the updated records are added to the SCD2 table
    INSERT INTO
       Orders_DIMENSIONAL_DW.dbo.DimRegion (RegionKey, RegionDescription, EffectiveDate, CurrentIndicator) 
       SELECT
          RegionID,
          RegionDescription,
          @Today,
          1 
       FROM
          (
             -- Merge statement
             MERGE INTO Orders_DIMENSIONAL_DW.dbo.DimRegion AS DST 
         USING proj2_db.dbo.Region AS SRC 
             ON (SRC.RegionID = DST.RegionKey)       
         -- New records inserted
             WHEN
                NOT MATCHED 
             THEN
                INSERT (RegionID, RegionDescription, EffectiveDate, CurrentIndicator) --There is no ValidTo
          VALUES
             (
                SRC.RegionID, SRC.RegionDescription, @Today, 1
             )
             -- Existing records updated if data changes
          WHEN
             MATCHED 
             AND CurrentIndicator = 1 
             AND 
             (
                ISNULL(DST.RegionID, '') <> ISNULL(SRC.RegionID, '') 
                OR ISNULL(DST.RegionDescription, '') <> ISNULL(SRC.RegionDescription, '') 
             )
             -- Update statement for a changed dimension record, to flag as no longer active
          THEN
             UPDATE
             SET
                DST.CurrentIndicator = 0, 
          DST.IneffectiveDate = @Yesterday 
          OUTPUT SRC.RegionID, SRC.RegionDescription, $Action AS MergeAction 
          )
          AS MRG 
       WHERE
          MRG.MergeAction = 'UPDATE' ;
        """
    )

    cursor_ER.execute(f'dbo.DimRegion_ETL;')

    cursor_ER.commit()
    cursor_ER.close()
    conn_ER.close()





def populate_dim_employee_etl(db='Orders_DIMENSIONAL_DW'):
    """
    Populate The Dim Employees table from the Relational_Database
    """
    # Call to read the configuration file
    c_ER = readconfig.get_sql_config(r'SQL_Server_Config.cfg', "Database2")
    # Create a connection string for SQL Server
    conn_info_ER = 'Driver={};Server={};Database={};Trusted_Connection={};'.format(*c_ER)
    # Connect to the server and to the desired database
    conn_ER = pyodbc.connect(conn_info_ER)
    # Create a Cursor class instance for executing T-SQL statements
    cursor_ER = conn_ER.cursor()

    cursor_ER.execute(f"use {db}")
    cursor_ER.execute(f'DROP PROCEDURE IF EXISTS dbo.DimEmployee_ETL;')
    cursor_ER.execute(
        """
        CREATE PROCEDURE dbo.DimEmployee_ETL
        AS
        DECLARE @Yesterday INT = 
        (
           YEAR(DATEADD(dd, - 1, GETDATE())) * 10000
        )
        + (MONTH(DATEADD(dd, - 1, GETDATE())) * 100) + DAY(DATEADD(dd, - 1, GETDATE())) 
        DECLARE @Today INT = 
        (
           YEAR(GETDATE()) * 10000
        )
        + (MONTH(GETDATE()) * 100) + DAY(GETDATE()) 
        INSERT INTO
           Orders_DIMENSIONAL_DW.dbo.DimEmployees (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, Birthdate, HireDate, Address,City,Region,PostalCode,Country,HomePhone,Extension,Photo,Notes,ReportsTo,EffectiveDate,CurrentIndicator) 
           SELECT
              EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, Birthdate, HireDate, Address,City,Region,PostalCode,Country,HomePhone,Extension,Photo,Notes,ReportsTo,
              @Today,
              1 
           FROM
              (
                 MERGE INTO Orders_DIMENSIONAL_DW.dbo.DimEmployees AS DST 
             USING proj2_db.dbo.Employees AS SRC 
                 ON (SRC.EmployeeID = DST.EmployeeID)
                 WHEN
                    NOT MATCHED 
                 THEN
                    INSERT (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, Birthdate, HireDate, Address,City,Region,PostalCode,Country,HomePhone,Extension,Photo,Notes,ReportsTo, EffectiveDate, CurrentIndicator) --There is no ValidTo
              VALUES
                 (
                    SRC.EmployeeID, SRC.LastName, SRC.FirstName, SRC.Title, SRC.TitleOfCourtesy, SRC.Birthdate, SRC.HireDate, SRC.Address,SRC.City,SRC.Region,SRC.PostalCode,SRC.Country,SRC.HomePhone,SRC.Extension,SRC.Photo,SRC.Notes,SRC.ReportsTo, @Today, 1
                 )
                 -- Existing records updated if data changes
              WHEN
                 MATCHED 
                 AND CurrentIndicator = 1 
                 AND 
                 (
                    ISNULL(DST.EmployeeID, '') <> ISNULL(SRC.EmployeeID, '') 
                    OR ISNULL(DST.LastName, '') <> ISNULL(SRC.LastName, '')
                    OR ISNULL(DST.FirstName, '') <> ISNULL(SRC.FirstName, '') 
                    OR ISNULL(DST.Title, '') <> ISNULL(SRC.Title, '') 
                    OR ISNULL(DST.TitleOfCourtesy, '') <> ISNULL(SRC.TitleOfCourtesy, '') 
                    OR ISNULL(DST.Birthdate, '') <> ISNULL(SRC.Birthdate, '') 
                    OR ISNULL(DST.HireDate, '') <> ISNULL(SRC.HireDate, '') 
                    OR ISNULL(DST.Address, '') <> ISNULL(SRC.Address, '') 
                    OR ISNULL(DST.City, '') <> ISNULL(SRC.City, '') 
                    OR ISNULL(DST.Region, '') <> ISNULL(SRC.Region, '')
                    OR ISNULL(DST.PostalCode, '') <> ISNULL(SRC.PostalCode, '') 
                    OR ISNULL(DST.Country, '') <> ISNULL(SRC.Country, '')
                    OR ISNULL(DST.HomePhone, '') <> ISNULL(SRC.HomePhone, '') 
                    OR ISNULL(DST.Extension, '') <> ISNULL(SRC.Extension, '') 
                    OR ISNULL(cast(cast(DST.Photo as varbinary(max)) as varchar(max)), '') != ISNULL(cast(cast(SRC.Photo as varbinary(max)) as varchar(max)), '')
                    OR ISNULL(DST.ReportsTo, '') <> ISNULL(SRC.ReportsTo, '')
                 )
                 -- Update statement for a changed dimension record, to flag as no longer active
              THEN
                 UPDATE
                 SET
                    DST.CurrentIndicator = 0, 
              DST.IneffectiveDate = @Yesterday 
              OUTPUT SRC.EmployeeID, SRC.LastName, SRC.FirstName, SRC.Title, SRC.TitleOfCourtesy, SRC.Birthdate, SRC.HireDate, SRC.Address,SRC.City,SRC.Region,SRC.PostalCode,SRC.Country,SRC.HomePhone,SRC.Extension,SRC.Photo,SRC.Notes,SRC.ReportsTo, $Action AS MergeAction 
              )
              AS MRG 
           WHERE
              MRG.MergeAction = 'UPDATE' ;
                """
    )
    cursor_ER.execute(f'dbo.DimEmployee_ETL;')

    cursor_ER.commit()
    cursor_ER.close()
    conn_ER.close()


if __name__ == '__main__':
    populate_dim_shippers_etl()
    # populate_dim_region_etl()
    populate_dim_employee_etl()
