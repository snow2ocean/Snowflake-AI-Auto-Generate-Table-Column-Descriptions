USE ROLE CORTEX_ROLE;
USE DATABASE CORTEX_DB;
USE SCHEMA CORTEX_SCM;
CREATE TABLE IF NOT EXISTS ORDERS AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS;
CREATE TABLE IF NOT EXISTS CUSTOMER AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;
CREATE TABLE IF NOT EXISTS SUPPLIER AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER;

desc table ORDERS;
desc table CUSTOMER;
desc table SUPPLIER;

-- Define columns and table description for single table
CALL DESCRIBE_SINGLE_TABLE_SET_COMMENT('CORTEX_DB', 'CORTEX_SCM', 'ORDERS', true, true);

desc table ORDERS;
desc table CUSTOMER;
desc table SUPPLIER;

-- Define columns and table description for all tables within schema
CALL DESCRIBE_TABLES_SET_COMMENT('CORTEX_DB', 'CORTEX_SCM',  true, true);

desc table ORDERS;
desc table CUSTOMER;
desc table SUPPLIER;

-----  -- Define columns and table description for all tables within schema and store them in catalog table

CREATE OR REPLACE TABLE catalog_table (
  domain VARCHAR,
  description VARCHAR,
  name VARCHAR,
  database_name VARCHAR,
  schema_name VARCHAR,
  table_name VARCHAR
  );


CALL DESCRIBE_TABLES_SET_CATALOG('CORTEX_DB', 'CORTEX_SCM', 'catalog_table');