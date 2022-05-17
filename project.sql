CREATE DATABASE proj2_db
	ON (NAME = proj2_mdf,
	FILENAME = 'C:\AUA\Business Intelligence\Group Project 2\proj2_mdf.mdf');

DROP TABLE IF EXISTS EmployeeTerritories;
DROP TABLE IF EXISTS Territories;
DROP TABLE IF EXISTS Region;
DROP TABLE IF EXISTS Categories;
DROP TABLE IF EXISTS Suppliers;
DROP TABLE IF EXISTS Products;
DROP TABLE IF EXISTS Orders
DROP TABLE IF EXISTS OrderDetails;
DROP TABLE IF EXISTS Employees;
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Shippers;

DROP TABLE IF EXISTS Region;
CREATE TABLE Region (
	RegionID int  not null PRIMARY KEY,
	RegionDescription nchar(50) not null
);

DROP TABLE IF EXISTS Territories;
create table Territories(


TerritoryID nvarchar(20) not null PRIMARY KEY,
TerritoryDescription nchar(50) not null,
RegionID int not null REFERENCES Region(RegionID)

);


DROP TABLE IF EXISTS Suppliers;
create table Suppliers(

SupplierID int not null PRIMARY KEY,
CompanyName nvarchar(40) not null,
ContactName nvarchar(30) null,
ContactTitle nvarchar(30) null,
Address nvarchar(60) null,
City nvarchar(15) null,
Region nvarchar(15) null,
PostalCode nvarchar(10) null,
Country nvarchar(15) null,
Phone nvarchar(24) null,
Fax nvarchar(24) null,
HomePage varchar(MAX) null

);

DROP TABLE IF EXISTS Categories;
create table Categories(

CategoryID int not null PRIMARY KEY,
CategoryName nvarchar(15) not null,
Description_ varchar(MAX) null,
Picture image null

);

DROP TABLE IF EXISTS Products;
create table Products(

ProductID int PRIMARY KEY,
ProductName nvarchar(40) null,
SupplierID int not null REFERENCES Suppliers(SupplierID),
CategoryID int not null REFERENCES Categories(categoryID),
QuantityPerProduct nvarchar(20) not null,
UnitPrice money not null,
UnitsInStock smallint not null,
UnitsOnOrder smallint not null,
ReorderLevel smallint not null,
Discontinued bit null,

);

DROP TABLE IF EXISTS Employees;
create table Employees(


EmployeeID int not null PRIMARY KEY,
LastName nvarchar(20) not null,
FirstName nvarchar(10) not null,
Title nvarchar(30) null,
TitleOfCourtesy nvarchar(25) null,
BirthDate datetime null,
HireDate datetime null,
Address_ nvarchar(60) null,
City nvarchar(15) null,
Region nvarchar(25) null,
PostalCode nvarchar(10) null,
Country nvarchar(15) null,
HomePhone nvarchar(24) null,
Extension nvarchar(4) null,
Photo image null,
Notes varchar(MAX) null,
ReportsTo int null REFERENCES Employees(EmployeeID),
PhotoPath nvarchar(255) null

);


DROP TABLE IF EXISTS EmployeeTerritories;
create table EmployeeTerritories(

EmployeeID int not null REFERENCES Employees(EmployeeID),
TerritoryID nvarchar(20) not null References Territories(TerritoryID),
 PRIMARY KEY(EmployeeID, TerritoryID)

);


DROP TABLE IF EXISTS Customers;
create table Customers(

CustomerID nchar(5) not null PRIMARY KEY,
CompanyName nvarchar(40) not null,
ContactName nvarchar(30) null,
ContactTitle nvarchar(30) null,
Address_ nvarchar(60) null,
City nvarchar(15) null,
Region nvarchar(15) null,
PostalCode nvarchar(10) null,
Country nvarchar(15) null,
Phone nvarchar(24) null,
Fax nvarchar(24) null
);

DROP TABLE IF EXISTS Shippers;
create table Shippers(

ShipperID int not null PRIMARY KEY,
CompanyName nvarchar(40) not null,
Phone nvarchar(24) null

);

DROP TABLE IF EXISTS Orders;
create table Orders(

OrderID int not null PRIMARY KEY,
CustomerID nchar(5) null REFERENCES Customers(CustomerID),
EmployeedID int null REFERENCES Employees(EmployeeID),
OrderDate datetime null,
RequiredDate datetime null,
ShippedDate datetime null,
ShipVisa int null REFERENCES Shippers(ShipperID),
Freight money null,
ShipName nvarchar(40) null,
ShipAddress nvarchar(60) null,
ShipCity nvarchar(15) null,
ShipRegion nvarchar(25) null,
ShipPostalCode nvarchar(10) null,
ShipCountry nvarchar(15) null
);

DROP TABLE IF EXISTS OrderDetails;
create table OrderDetails(

OrderID int not null REFERENCES Orders(OrderID),
ProductID int not null REFERENCES Products(ProductID),
UnitPrice money not null,
Quantity smallint not null,
Discount real not null,
Primary key(OrderID, ProductID)

);

select * from Categories;

SELECT * FROM sys.tables WHERE schema_id = 1;