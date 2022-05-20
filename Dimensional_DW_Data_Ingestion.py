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
    cursor_ER.execute(f'ALTER TABLE DimTerritories NOCHECK CONSTRAINT region_id_fk;')
    cursor_ER.execute(
        f"""
        CREATE PROCEDURE dbo.DimRegion_ETL
        AS
        -- Define the dates used in validity - assume whole 24 hour cycles
        DECLARE @Yesterday bigint =  --20210412 = 2021 * 10000 + 4 * 100 + 12
        (
           YEAR(DATEADD(dd, - 1, GETDATE())) * 1000
        )
        + (MONTH(DATEADD(dd, - 1, GETDATE())) * 100) + DAY(DATEADD(dd, - 1, GETDATE())) 
        DECLARE @Today bigint = --20210413 = 2021 * 10000 + 4 * 100 + 13
        (
           YEAR(GETDATE()) * 1000
        )
        + (MONTH(GETDATE()) * 100) + DAY(GETDATE()) -- Outer insert - the updated records are added to the SCD2 table
        INSERT INTO
           Orders_DIMENSIONAL_DW.dbo.DimRegion (RegionKey, RegionDescription, EffectiveDate, CurrentIndicator) 
           SELECT
              RegionId,
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
                    INSERT (RegionKey, RegionDescription, EffectiveDate, CurrentIndicator) --There is no ValidTo
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

def populate_fact_orders_etl(db='Orders_DIMENSIONAL_DW'):
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
    cursor_ER.execute(f"DROP PROCEDURE IF EXISTS dbo.fact_orders_etl;")
    cursor_ER.execute(f'ALTER TABLE FactOrders NOCHECK CONSTRAINT all;')
    cursor_ER.execute(
        f"""
        create procedure fact_orders_etl
        as
        insert into FactOrders
        select Orders.OrderID,customerkey,Employees.EmployeeID,CONVERT(INT, CAST (OrderDate as DATETIME)),CONVERT(INT, CAST (RequiredDate as DATETIME)),CONVERT(INT, CAST (ShippedDate as DATETIME)),ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,ShipCountry,products.ProductID,products.UnitPrice,Quantity,Discount 
        from 
        proj2_db.dbo.Region
        INNER JOIN proj2_db.dbo.Territories ON proj2_db.dbo.Region.RegionID = proj2_db.dbo.Territories.RegionID
        INNER JOIN proj2_db.dbo.EmployeeTerritories ON proj2_db.dbo.Territories.TerritoryID = proj2_db.dbo.EmployeeTerritories.TerritoryID
        INNER JOIN proj2_db.dbo.Employees ON proj2_db.dbo.Employees.EmployeeID = proj2_db.dbo.EmployeeTerritories.EmployeeID
        INNER JOIN proj2_db.dbo.Orders ON proj2_db.dbo.Employees.EmployeeID = proj2_db.dbo.Orders.EmployeeID
        INNER JOIN proj2_db.dbo.Shippers ON proj2_db.dbo.Orders.ShipVia = proj2_db.dbo.Shippers.ShipperID
        INNER JOIN proj2_db.dbo.Customers ON proj2_db.dbo.Orders.CustomerID = proj2_db.dbo.Customers.CustomerID
        INNER JOIN proj2_db.dbo.OrderDetails ON proj2_db.dbo.Orders.OrderID = proj2_db.dbo.OrderDetails.OrderID
        INNER JOIN proj2_db.dbo.Products ON proj2_db.dbo.OrderDetails.ProductID = proj2_db.dbo.Products.ProductID
        INNER JOIN proj2_db.dbo.Suppliers ON proj2_db.dbo.Products.SupplierID = proj2_db.dbo.Suppliers.SupplierID
        INNER JOIN proj2_db.dbo.Categories ON proj2_db.dbo.Products.CategoryID = proj2_db.dbo.Categories.CategoryID
        INNER JOIN DimCustomers ON DimCustomers.CustomerID = proj2_db.dbo.Customers.CustomerID;
        """
    )

    cursor_ER.execute(f'dbo.fact_orders_etl;')

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
    cursor_ER.execute(f'DROP PROCEDURE IF EXISTS dbo.DimEmployees_ETL;')
    cursor_ER.execute(f'ALTER TABLE BridgeEmployeeTerritories NOCHECK CONSTRAINT employee_id_fk;')

    cursor_ER.execute(
        """
        CREATE PROCEDURE dbo.DimEmployees_ETL
        AS
        -- Define the dates used in validity - assume whole 24 hour cycles
        DECLARE @Yesterday INT =  --20210412 = 2021 * 10000 + 4 * 100 + 12
        (
           YEAR(DATEADD(dd, - 1, GETDATE())) * 1000
        )
        + (MONTH(DATEADD(dd, - 1, GETDATE())) * 100) + DAY(DATEADD(dd, - 1, GETDATE())) 
        DECLARE @Today INT = --20210413 = 2021 * 10000 + 4 * 100 + 13
        (
           YEAR(GETDATE()) * 1000
        )
        + (MONTH(GETDATE()) * 100) + DAY(GETDATE()) -- Outer insert - the updated records are added to the SCD2 table
        INSERT INTO
           Orders_DIMENSIONAL_DW.dbo.DimEmployees (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, Birthdate, HireDate, Address,City,Region,PostalCode,Country,HomePhone,Extension,Photo,Notes,ReportsTo,EffectiveDate,CurrentIndicator) 
           SELECT
              EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, Birthdate, HireDate, Address,City,Region,PostalCode,Country,HomePhone,Extension,Photo,Notes,ReportsTo,
              @Today,
              1 
           FROM
              (
                 -- Merge statement
                 MERGE INTO Orders_DIMENSIONAL_DW.dbo.DimEmployees AS DST 
             USING proj2_db.dbo.Employees AS SRC 
                 ON (SRC.EmployeeID = DST.EmployeeID)       
             -- New records inserted
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
                     ISNULL(DST.LastName, '') <> ISNULL(SRC.LastName, '')
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
                    OR ISNULL(convert(varchar,convert(varbinary,DST.Photo)), '') <> ISNULL(convert(varchar,convert(varbinary,SRC.Photo)), '')
                    OR ISNULL(DST.Notes, '') <> ISNULL(SRC.Notes, '') 
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
    cursor_ER.execute(f'dbo.DimEmployees_ETL;')

    cursor_ER.commit()
    cursor_ER.close()
    conn_ER.close()

def populate_dim_territories_etl(db='Orders_DIMENSIONAL_DW'):
    """
    Populate The Dim Territories table from the Relational_Database
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
    cursor_ER.execute(f'DROP PROCEDURE IF EXISTS dbo.DimTerritories_ETL;')
    cursor_ER.execute(f'ALTER TABLE BridgeEmployeeTerritories NOCHECK CONSTRAINT territory_id_fk;')

    cursor_ER.execute(
        """
        CREATE PROCEDURE dbo.DimTerritories_ETL
        AS
        -- Define the dates used in validity - assume whole 24 hour cycles
        DECLARE @Yesterday INT =  --20210412 = 2021 * 10000 + 4 * 100 + 12
        (
           YEAR(DATEADD(dd, - 1, GETDATE())) * 1000
        )
        + (MONTH(DATEADD(dd, - 1, GETDATE())) * 100) + DAY(DATEADD(dd, - 1, GETDATE())) 
        DECLARE @Today INT = --20210413 = 2021 * 10000 + 4 * 100 + 13
        (
           YEAR(GETDATE()) * 1000
        )
        + (MONTH(GETDATE()) * 100) + DAY(GETDATE()) -- Outer insert - the updated records are added to the SCD2 table
        INSERT INTO
           Orders_DIMENSIONAL_DW.dbo.DimTerritories (TerritoryKey, TerritoryDescription, EffectiveDate, CurrentIndicator) 
           SELECT
              RegionID,
              TerritoryDescription,
              @Today,
              1 
           FROM
              (
                 -- Merge statement
                 MERGE INTO Orders_DIMENSIONAL_DW.dbo.DimTerritories AS DST 
             USING proj2_db.dbo.Territories AS SRC 
                 ON (SRC.TerritoryID = DST.TerritoryKey)       
             -- New records inserted
                 WHEN
                    NOT MATCHED 
                 THEN
                    INSERT (TerritoryKey, TerritoryDescription, RegionID, EffectiveDate, CurrentIndicator) --There is no ValidTo
              VALUES
                 (
                    SRC.TerritoryID, SRC.TerritoryDescription, SRC.RegionID, @Today, 1
                 )
                 -- Existing records updated if data changes
              WHEN
                 MATCHED 
                 AND CurrentIndicator = 1 
                 AND 
                 (
                    ISNULL(DST.TerritoryID, '') <> ISNULL(SRC.TerritoryID, '') 
                    OR ISNULL(DST.TerritoryDescription, '') <> ISNULL(SRC.TerritoryDescription, '') 
                    OR ISNULL(DST.RegionID, '') <> ISNULL(SRC.RegionID, '')
                 )
                 -- Update statement for a changed dimension record, to flag as no longer active
              THEN
                 UPDATE
                 SET
                    DST.CurrentIndicator = 0, 
              DST.IneffectiveDate = @Yesterday 
              OUTPUT SRC.TerritoryID, SRC.TerritoryDescription, SRC.RegionID ,$Action AS MergeAction 
              )
              AS MRG 
           WHERE
              MRG.MergeAction = 'UPDATE' ;
                """
    )
    cursor_ER.execute(f'dbo.DimTerritories_ETL;')

    cursor_ER.commit()
    cursor_ER.close()
    conn_ER.close()


def populate_dim_customers_etl(db='Orders_DIMENSIONAL_DW'):
    """
    Populate The Dim Territories table from the Relational_Database
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
    cursor_ER.execute(f'DROP PROCEDURE IF EXISTS dbo.DimCustomers_ETL;')
    # cursor_ER.execute(f'ALTER TABLE BridgeEmployeeTerritories NOCHECK CONSTRAINT territory_id_fk;')

    cursor_ER.execute(
        """
        CREATE PROCEDURE dbo.DimCustomers_ETL
        AS  
         DECLARE @Yesterday VARCHAR(8) = CAST(YEAR(DATEADD(dd,-1,GETDATE())) AS CHAR(4)) + RIGHT('0' + CAST(MONTH(DATEADD(dd,-1,GETDATE())) AS VARCHAR(2)),2) + RIGHT('0' + CAST(DAY(DATEADD(dd,-1,GETDATE())) AS VARCHAR(2)),2)
         --20210413: string/text/char
        MERGE dbo.DimCustomers AS DST
        USING proj2_db.dbo.Customers AS SRC
        ON (SRC.CustomerID = DST.CustomerID)
        WHEN NOT MATCHED THEN
        INSERT (CustomerID, CompanyName, ContactName, ContactTitle, Address,City,Region,PostalCode,Country,Phone,Fax)
        VALUES (SRC.CustomerID, SRC.CompanyName, SRC.ContactName, SRC.ContactTitle, SRC.Address, SRC.City, SRC.Region, SRC.PostalCode, SRC.Country, SRC.Phone, SRC.Fax)
        WHEN MATCHED  -- there can be only one matched case
        AND (DST.CustomerID <> SRC.CustomerID
         OR DST.CompanyName <> SRC.CompanyName
         OR DST.ContactName <> SRC.ContactName
         OR DST.ContactTitle <> SRC.ContactTitle
         OR DST.Address <> SRC.Address
         OR DST.City <> SRC.City
         OR DST.Region <> SRC.Region
         OR DST.PostalCode <> SRC.PostalCode
         OR DST.Country <> SRC.Country
         OR DST.Phone <> SRC.Phone
         OR DST.Fax <> SRC.Fax
         )
         
        THEN 
          UPDATE 
          SET  DST.CompanyName = SRC.CompanyName -- simple overwrite
            ,DST.ContactName = SRC.ContactName
            ,DST.ContactTitle = SRC.ContactTitle
            ,DST.Address = SRC.Address
            ,DST.Address_prev = (CASE WHEN DST.Address <> SRC.Address THEN DST.Address ELSE DST.Address_prev END)
            ,DST.Address_prev_validto = (CASE WHEN DST.Address <> SRC.Address THEN @Yesterday ELSE DST.Address_prev_validto END)
            ,DST.City = SRC.City
            ,DST.Region = SRC.Region
            ,DST.Phone = SRC.Phone
            ,DST.Fax = SRC.Fax;
                """
    )
    cursor_ER.execute(f'dbo.DimCustomers_ETL;')

    cursor_ER.commit()
    cursor_ER.close()
    conn_ER.close()

def populate_dim_date_etl(db='Orders_DIMENSIONAL_DW'):
    """
    Populate The Date table from the Relational_Database
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
    cursor_ER.execute(f'drop procedure if exists dbo.fill_dimdate;')
    cursor_ER.execute(f'set identity_insert date on;')

    cursor_ER.execute(
        """
        create procedure dbo.fill_dimdate (@start_date DATE,@end_date DATE)
        as 
        begin try
        DECLARE @LoopDate datetime
        SET @LoopDate = @start_date
        
        WHILE @LoopDate <= @end_date
        BEGIN
         -- add a record into the date dimension table for this date
         INSERT INTO Date(datekey,date,day,Month,Quarter,Year,MonthName,WeekOfYear,WeekOfMonth,DayOfMonth,DayOfYear) VALUES (
            Year(@LoopDate) * 10000 +  Month(@LoopDate) * 100 + Day(@LoopDate)
            ,@LoopDate
            ,Day(@LoopDate)
            ,Month(@LoopDate),
            CASE WHEN Month(@LoopDate) IN (1, 2, 3) THEN 1
                WHEN Month(@LoopDate) IN (4, 5, 6) THEN 2
                WHEN Month(@LoopDate) IN (7, 8, 9) THEN 3
                WHEN Month(@LoopDate) IN (10, 11, 12) THEN 4 end, 
            Year(@LoopDate),
            Datename(m,@LoopDate),
            DATEPART(week, @LoopDate),
            (DATEPART(day,@LoopDate)-1)/7 + 1,
            day(@LoopDate),
            DATEPART (dayofyear , @LoopDate)
        )
         SET @LoopDate = DateAdd(d, 1, @LoopDate)
         END
        END TRY
        BEGIN CATCH
            THROW
        END CATCH;

                """
    )
    cursor_ER.execute(f"exec dbo.fill_dimdate @start_date='1950-01-01',@end_date='2022-05-20';")
    cursor_ER.execute(f"set identity_insert date off;")


    cursor_ER.commit()
    cursor_ER.close()
    conn_ER.close()


if __name__ == '__main__':
    populate_dim_shippers_etl()
    populate_dim_employee_etl()
    populate_dim_region_etl()
    populate_dim_territories_etl()
    populate_dim_customers_etl()
    populate_fact_orders_etl()
    populate_dim_date_etl()
