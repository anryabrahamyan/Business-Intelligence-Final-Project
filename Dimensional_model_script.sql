CREATE database Orders_DIMENSIONAL_DW ON (NAME=Orders_DIMENSIONAL_DW, FILENAME='C:\Users\Nitro\Desktop\BI group project\BI_final_project\orders_dim_db.mdf');

use Orders_DIMENSIONAL_DW;


DROP TABLE IF EXISTS DimRegion;
DROP TABLE IF EXISTS DimTerritories;
DROP TABLE IF EXISTS BridgeEmployeeTerritories;
DROP TABLE IF EXISTS DimEmployees;
DROP TABLE IF EXISTS DimShippers;
DROP TABLE IF EXISTS DimProducts;
DROP TABLE IF EXISTS DimProducts_History;
DROP TABLE IF EXISTS DimCustomers;
DROP TABLE IF EXISTS Date;
DROP TABLE IF EXISTS FactOrders;


create table DimRegion(

  RegionKey int not null,
  RegionID int IDENTITY(1,1) PRIMARY KEY,
  RegionDescription nchar(50) not null,
  EffectiveDate datetime,
  IneffectiveDate datetime,
  CurrentIndicator bit


);

create table DimTerritories(
  TerritoryKey int not null,
  TerritoryID int IDENTITY(1,1)  PRIMARY KEY,
  TerritoryDescription nchar(50) not null,
  RegionID int not null,
  EffectiveDate datetime,
  IneffectiveDate datetime,
  CurrentIndicator bit,
  CONSTRAINT region_id_fk FOREIGN KEY (RegionID) REFERENCES DimRegion(RegionID)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);


create table DimEmployees(

  EmployeeKey int IDENTITY(1,1) PRIMARY KEY,
  EmployeeID int not null UNIQUE,
  LastName nvarchar(20) not null,
  FirstName nvarchar(10) not null,
  Title nvarchar(30),
  TitleOfCourtesy nvarchar(25),
  Birthdate datetime,
  HireDate datetime,
  Address nvarchar(60),
  City nvarchar(15),
  Region nvarchar(15),
  PostalCode nvarchar(15),
  Country nvarchar(15),
  HomePhone nvarchar(24),
  Extension nvarchar(4),
  Photo image,
  Notes varchar(MAX),
  ReportsTo int,
  PhotoPath nvarchar(255),
  EffectiveDate datetime,
  IneffectiveDate datetime,
  CurrentIndicator bit

);

create table BridgeEmployeeTerritories(

  EmployeeID int not null, 
  TerritoryID int not null,
  CONSTRAINT PK_Person PRIMARY KEY (EmployeeID,TerritoryID),
  CONSTRAINT employee_id_fk FOREIGN KEY (EmployeeID) REFERENCES DimEmployees(EmployeeID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
  CONSTRAINT territory_id_fk FOREIGN KEY (TerritoryID) REFERENCES DimTerritories(TerritoryID)
        ON DELETE CASCADE
        ON UPDATE CASCADE

);

create table DimShippers(

  ShipperKey int IDENTITY(1,1) PRIMARY KEY,
  ShipperID int not null,
  CompanyName nvarchar(40) not null,
  Phone nvarchar(24)

);




create table DimProducts(

  ProductKey int IDENTITY(1,1) PRIMARY KEY,
  ProductID int not null,
  ProductName nvarchar(40) not null,
  QuantityPerUnit nvarchar(20),
  UnitPrice money,
  UnitsInStock smallint,
  UnitsOnOrder smallint,
  ReorderLevel smallint,
  Discontinued bit not null,
  CategoryName nvarchar(15) not null,
  CategoryDescription varchar(MAX),
  SupplierCompanyName nvarchar(40),
  SupplierContactTitle nvarchar(30),
  SupplierContactName nvarchar(30),
  SupplierCity nvarchar(15),
  SupplierRegion nvarchar(15),
  SupplierCountry nvarchar(15),
  SupplierPostalCode nvarchar(15)

);


create table DimProducts_History(

  ProductKey int IDENTITY(1,1) PRIMARY KEY,
  ProductID int not null,
  ProductName nvarchar(40) not null,
  QuantityPerUnit nvarchar(20),
  UnitPrice money,
  UnitsInStock smallint,
  UnitsOnOrder smallint,
  ReorderLevel smallint,
  Discontinued bit not null,
  CategoryName nvarchar(15) not null,
  CategoryDescription varchar(MAX),
  SupplierCompanyName nvarchar(40),
  SupplierContactTitle nvarchar(30),
  SupplierContactName nvarchar(30),
  SupplierCity nvarchar(15),
  SupplierRegion nvarchar(15),
  SupplierCountry nvarchar(15),
  SupplierPostalCode nvarchar(15),
  ValidFrom datetime,
  ValidTo datetime
);


create table DimCustomers(

  CustomerKey int IDENTITY(1,1) PRIMARY KEY,
  CustomerID nchar(5) not null,
  CompanyName nvarchar(40) not null,
  ContactName nvarchar(30),
  ContactTitle nvarchar(30),
  Address nvarchar(60),
  Address_prev nvarchar(60),
  Address_prev_validto char(8),
  City nvarchar(15),
  Region nvarchar(15),
  PostalCode nvarchar(10),
  Country nvarchar(15),
  Phone nvarchar(24),
  Fax nvarchar(24)

);

create table Date(

  DateKey int IDENTITY(1,1),
  Date datetime not null,
  Day int not null,
  Month int not null,
  Quarter int not null,
  Year int not null,
  MonthName nvarchar(10),
  WeekOfYear int not null,
  WeekOfMonth int not null,
  DayOfMonth int not null,
  DayOfYear int not null
  constraint datekey_pk primary key (DateKey)
);

create table FactOrders(

  Order_SK int PRIMARY KEY IDENTITY(1,1),
  OrderID int not null,
  CustomerKey int not null REFERENCES DimCustomers(CustomerKey),
  EmployeeKey int not null,
  OrderDateKey int not null REFERENCES Date(Datekey),
  RequiredDate int not null REFERENCES Date(Datekey),
  ShippedDate int REFERENCES Date(Datekey), --not null
  ShipperKey int not null REFERENCES DimShippers(ShipperKey),
  Freight money,
  ShipName nvarchar(40),
  ShipAddress nvarchar(60),
  ShipCity nvarchar(15),
  ShipRegion nvarchar(15),
  ShipPostalCode nvarchar(10),
  ShipCountry nvarchar(15),
  ProductKey int not null REFERENCES DimProducts(ProductKey),
  UnitPrice money not null,
  Quantity smallint not null,
  Discount real not null,
);


