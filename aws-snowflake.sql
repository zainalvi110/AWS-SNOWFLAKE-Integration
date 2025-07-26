----------------------------------------------------------------------------------
--         SNOWFLAKE STAGES HANDS-ON  -- DATA LOADING AND UNLOADING LAB         --
----------------------------------------------------------------------------    ---

---> set the Role
use role ACCOUNTADMIN;

---> set the Warehouse
use warehouse COMPUTE_WH;

---> create Database and Schemas
create database SLEEK_OMS;
create schema L1_LANDING;

---> Use Database and Schemas
use database SLEEK_OMS;
use schema L1_LANDING;

---> Create Tables
create table if not exists DATES_DIM (
date date,
day varchar(3),
month varchar(10),
year varchar(4),
quarter int null,
dayofweek varchar(10),
weekofyear int);

create table if not exists CUSTOMERS_DIM (
customerid varchar(10),
firstname varchar(50),
lastname varchar(50),
email varchar(100),
phone varchar(100),
address varchar(100),
city varchar(50),
state varchar(2),
zipcode varchar(10));

create table if not exists EMPLOYEES_DIM (
employeeid int,
firstname varchar(100),
lastname varchar(100),
email varchar(200),
jobtitle varchar(100),
hiredate date,
managerid int,
address varchar(200),
city varchar(50),
state varchar(50),
zipcode varchar(10));

create table if not exists STORES_DIM (
storeid int,
storename varchar(100),
address varchar(200),
city varchar(50),
state varchar(50),
zipcode varchar(10),
email varchar(200),
phone varchar(50));

---> List User and Table Stages
ls @~;
ls @%DATES_DIM;
ls @%CUSTOMERS_DIM;
ls @%EMPLOYEES_DIM;
ls @%STORES_DIM;

---> Create Named Internal Stage
CREATE OR REPLACE STAGE sales_team_int_stg;

---> List Named Internal Stage
ls @sales_team_int_stg

---> Create Named External Stage
---  Option 1: CREDENTIALS:
CREATE OR REPLACE STAGE oms_datalake_ext_stg
  URL='s3://s3explore/'
  CREDENTIALS=(AWS_KEY_ID='AKIA5SDAFSDAFKIXSQ' AWS_SECRET_KEY='ab68uuTVzL0oc4pNAgMA0eZdz')

---> List Named External Stage
ls @oms_datalake_ext_stg

---> Drop and recreate using option 2
DROP STAGE oms_datalake_ext_stg;

---  Option 2: STORAGE INTEGRATION:
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::93324327689925:role/lab_role'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://s3explore/');

DESCRIBE INTEGRATION s3_integration;

CREATE STAGE oms_datalake_ext_stg
  URL = 's3://s3explore'
  STORAGE_INTEGRATION = s3_integration;

---> List Named External Stage
ls @oms_datalake_ext_stg


---> Put files in to internal Stages:
PUT 'file://C:/Users/mamba/Desktop/csvfiles/dates.csv' @~;
PUT 'file://C:/Users/mamba/Desktop/csvfiles/customers.csv' @%CUSTOMERS_DIM;
PUT 'file://C:/Users/mamba/Desktop/csvfiles/employees.csv' @sales_team_int_stg;

--Note: PUT/GET not supported for for external Stages.
--Note: Example for Linux or macOS: PUT file:///tmp/data/mydata.csv @~



ls @~;
ls @%CUSTOMERS_DIM;
ls @sales_team_int_stg
ls @oms_datalake_ext_stg


---> Copy files from Stages to Tables
COPY INTO DATES_DIM
FROM @~
FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"',SKIP_HEADER = 1)
PURGE = TRUE;

---> Create file format
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

COPY INTO CUSTOMERS_DIM
FROM @%CUSTOMERS_DIM
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

COPY INTO EMPLOYEES_DIM
FROM (
    SELECT $1, $2, $3, $4, $5, TO_DATE($6, 'DD-MM-YYYY'), $7, $9, $10, $11, $8
    FROM @sales_team_int_stg
)
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

COPY INTO STORES_DIM
FROM @oms_datalake_ext_stg
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = SKIP_FILE
PATTERN='.*[.]csv';


---> Check if data loaded in to the tables:
SELECT * FROM DATES_DIM;
SELECT * FROM CUSTOMERS_DIM;
SELECT * FROM EMPLOYEES_DIM;
SELECT * FROM STORES_DIM;


---> Copy files from Tables to Stages
COPY INTO @%EMPLOYEES_DIM/csv_export/
FROM EMPLOYEES_DIM 
FILE_FORMAT = (TYPE = 'csv' COMPRESSION = 'GZIP');


COPY INTO @%EMPLOYEES_DIM/json_export/
FROM (
    SELECT OBJECT_CONSTRUCT(
               'employeeid', employeeid,
               'firstname', firstname,
               'lastname', lastname,
               'email', email,
               'jobtitle', jobtitle,
               'hiredate', hiredate,
               'managerid', managerid,
               'address', address,
               'city', city,
               'state', state,
               'zipcode', zipcode
           ) AS obj
    FROM EMPLOYEES_DIM
)
FILE_FORMAT = (TYPE = 'json' COMPRESSION = 'GZIP');


COPY INTO @oms_datalake_ext_stg/employees/
FROM (select employeeid, firstname, lastname from EMPLOYEES_DIM where managerid = 1)


---> GET files to your on-premises or local host.
GET @%EMPLOYEES_DIM/json_export 'file://C:/Users/mamba/Desktop/exportedfiles/';
--Note: PUT/GET not supported for for external Stages.


---> Remove all files from the /employees in a stage named mystage:
REMOVE @oms_datalake_ext_stg/employees;

---> Remove all files from the stage for the orders table:
REMOVE @%DATES_DIM;

---> Remove files whose names match the pattern *jun*:
RM @~ pattern='.*jun.*';