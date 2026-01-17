
-- ===============================
-- 1) Set variables (edit as needed)
-- ===============================
SET database_name  = 'CORTEX_DB';
SET schema_name    = 'CORTEX_SCM';
SET role_name      = 'CORTEX_ROLE';
SET warehouse_name = 'COMPUTE_WH';

-- capture current user to grant the role later
SET current_username = (SELECT CURRENT_USER());

-- ===============================
-- 2) Create database & schema (as SYSADMIN)
-- ===============================
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS IDENTIFIER($database_name);
USE DATABASE IDENTIFIER($database_name);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($schema_name);

-- ===============================
-- 3) Create role (as SECURITYADMIN)
-- ===============================
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS IDENTIFIER($role_name);

-- ===============================
-- 4) Privilege grants (as SYSADMIN - schema/db owner)
-- ===============================
USE ROLE SYSADMIN;

-- Base access to database and schema
GRANT USAGE ON DATABASE IDENTIFIER($database_name) TO ROLE IDENTIFIER($role_name);
USE DATABASE IDENTIFIER($database_name);
GRANT USAGE ON SCHEMA IDENTIFIER($schema_name) TO ROLE IDENTIFIER($role_name);

-- Optional: comprehensive schema privileges (comment out if you want least-privilege)
GRANT ALL ON SCHEMA IDENTIFIER($schema_name) TO ROLE IDENTIFIER($role_name);


-- Warehouse usage for running queries
GRANT USAGE ON WAREHOUSE IDENTIFIER($warehouse_name) TO ROLE IDENTIFIER($role_name);

-- ===============================
-- 5) Grant the role to current user and SYSADMIN (as SECURITYADMIN)
-- ===============================
USE ROLE SECURITYADMIN;

-- grant to current user
GRANT ROLE IDENTIFIER($role_name) TO USER IDENTIFIER($current_username);

-- grant to SYSADMIN (so SYSADMIN can switch into it if needed)
GRANT ROLE IDENTIFIER($role_name) TO ROLE SYSADMIN;

-- ===============================
-- 6) Create the Python stored procedure as the new role
--    (Requires: USAGE on DB/SCHEMA and CREATE PROCEDURE on schema)
-- ===============================
-- It's safer to use EXECUTE IMMEDIATE to switch to a dynamic role name
USE ROLE IDENTIFIER($role_name);
USE WAREHOUSE IDENTIFIER($warehouse_name);
USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);

-- Procedure 1 for single table
CREATE OR REPLACE PROCEDURE DESCRIBE_SINGLE_TABLE_SET_COMMENT (
  database_name STRING,
  schema_name STRING,
  table_name STRING,
  set_table_comment BOOLEAN,
  set_column_comment BOOLEAN
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','joblib')
HANDLER = 'main'
AS
$$
import json
from joblib import Parallel, delayed
import multiprocessing

def generate_descr(session, database_name, schema_name, table, set_table_comment, set_column_comment):
  table_name =  table['TABLE_NAME']
  async_job = session.sql(f"CALL AI_GENERATE_TABLE_DESC( '{database_name}.{schema_name}.{table_name}',{{'describe_columns': true, 'use_table_data': true}})").collect_nowait()
  result = async_job.result()
  output = json.loads(result[0][0])
  columns_ret = output["COLUMNS"]
  table_ret = output["TABLE"][0]

  table_description = table_ret["description"]
  table_name = table_ret["name"]
  database_name = table_ret["database_name"]
  schema_name = table_ret["schema_name"]

  if (set_table_comment):
      table_description = table_description.replace("'", "\\'")
      session.sql(f"""ALTER TABLE {database_name}.{schema_name}.{table_name} SET COMMENT = '{table_description}'""").collect()

  for column in columns_ret:
      column_description = column["description"];
      column_name = column["name"];
      if not column_name.isupper():
        column_name = '"' + column_name + '"'

      if (set_column_comment):
          column_description = column_description.replace("'", "\\'")
          session.sql(f"""ALTER TABLE  {database_name}.{schema_name}.{table_name} MODIFY COLUMN {column_name}  COMMENT '{column_description}'""").collect()

  return 'Success';

def main(session, database_name, schema_name, table_name,set_table_comment, set_column_comment):

    schema_name = schema_name.upper()
    database_name = database_name.upper()
    table_name = table_name.upper()
    tablenames = session.sql(f"""SELECT table_name
                      FROM {database_name}.information_schema.tables
                      WHERE table_schema = '{schema_name}'
                      AND TABLE_NAME = '{table_name}'
                      AND table_type = 'BASE TABLE'""").collect()
    try:
        Parallel(n_jobs=multiprocessing.cpu_count(), backend="threading")(
                delayed(generate_descr)(
                    session,
                    database_name,
                    schema_name,
                    table,
                    set_table_comment,
                    set_column_comment,
                ) for table in tablenames
            )
        return 'Success'
    except Exception as e:
        # Catch and return the error message
        return f"An error occurred: {str(e)}"
$$;

-------
-- Procedure 2 for all tables within specfic schema
CREATE OR REPLACE PROCEDURE DESCRIBE_TABLES_SET_COMMENT (database_name STRING, schema_name STRING,
  set_table_comment BOOLEAN,
  set_column_comment BOOLEAN)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES=('snowflake-snowpark-python','joblib')
  HANDLER = 'main'
AS
$$
import json
from joblib import Parallel, delayed
import multiprocessing

def generate_descr(session, database_name, schema_name, table, set_table_comment, set_column_comment):
  table_name =  table['TABLE_NAME']
  async_job = session.sql(f"CALL AI_GENERATE_TABLE_DESC( '{database_name}.{schema_name}.{table_name}',{{'describe_columns': true, 'use_table_data': true}})").collect_nowait()
  result = async_job.result()
  output = json.loads(result[0][0])
  columns_ret = output["COLUMNS"]
  table_ret = output["TABLE"][0]

  table_description = table_ret["description"]
  table_name = table_ret["name"]
  database_name = table_ret["database_name"]
  schema_name = table_ret["schema_name"]

  if (set_table_comment):
      table_description = table_description.replace("'", "\\'")
      session.sql(f"""ALTER TABLE {database_name}.{schema_name}.{table_name} SET COMMENT = '{table_description}'""").collect()

  for column in columns_ret:
      column_description = column["description"];
      column_name = column["name"];
      if not column_name.isupper():
        column_name = '"' + column_name + '"'

      if (set_column_comment):
          column_description = column_description.replace("'", "\\'")
          session.sql(f"""ALTER TABLE  {database_name}.{schema_name}.{table_name} MODIFY COLUMN {column_name}  COMMENT '{column_description}'""").collect()

  return 'Success';

def main(session, database_name, schema_name, set_table_comment, set_column_comment):

    schema_name = schema_name.upper()
    database_name = database_name.upper()
    tablenames = session.sql(f"""SELECT table_name
                      FROM {database_name}.information_schema.tables
                      WHERE table_schema = '{schema_name}'
                      AND table_type = 'BASE TABLE'""").collect()
    try:
        Parallel(n_jobs=multiprocessing.cpu_count(), backend="threading")(
                delayed(generate_descr)(
                    session,
                    database_name,
                    schema_name,
                    table,
                    set_table_comment,
                    set_column_comment,
                ) for table in tablenames
            )
        return 'Success'
    except Exception as e:
        # Catch and return the error message
        return f"An error occurred: {str(e)}"
$$;



--------
-- Procedure 3 for all tables within schema and stored description in catalog table
CREATE OR REPLACE PROCEDURE DESCRIBE_TABLES_SET_CATALOG (database_name string, schema_name string, catalog_table string)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES=('snowflake-snowpark-python','joblib')
  HANDLER = 'main'
AS
$$
import json
from joblib import Parallel, delayed
import multiprocessing

def generate_descr(session, database_name, schema_name, table, catalog_table):
    table_name =  table['TABLE_NAME']
    async_job = session.sql(f"CALL AI_GENERATE_TABLE_DESC( '{database_name}.{schema_name}.{table_name}',{{'describe_columns': true, 'use_table_data': true}})").collect_nowait()
    result = async_job.result()
    output = json.loads(result[0][0])
    columns_ret = output["COLUMNS"]
    table_ret = output["TABLE"][0]

    table_description = table_ret["description"]
    table_description = table_description.replace("'", "\\'")
    table_name = table_ret["name"]
    database_name = table_ret["database_name"]
    schema_name = table_ret["schema_name"]

    session.sql(f"""INSERT INTO {catalog_table} (domain, description, name, database_name, schema_name, table_name)
                          VALUES ('TABLE', '{table_description}', '{table_name}', '{database_name}', '{schema_name}', null)""").collect()

    for column in columns_ret:
        column_description = column["description"];
        column_description = column_description.replace("'", "\\'")
        column_name = column["name"];
        if not column_name.isupper():
            column_name = '"' + column_name + '"'
        session.sql(f"""INSERT INTO {catalog_table} (domain, description, name, database_name, schema_name, table_name)
                          VALUES ('COLUMN', '{column_description}', '{column_name}', '{database_name}', '{schema_name}', '{table_name}')""").collect()

    return 'Success';

def main(session, database_name, schema_name, catalog_table):

    schema_name = schema_name.upper()
    database_name = database_name.upper()
    catalog_table_upper = catalog_table.upper()
    tablenames = session.sql(f"""SELECT table_name
                      FROM {database_name}.information_schema.tables
                      WHERE table_schema = '{schema_name}'
                      AND table_type = 'BASE TABLE'
                      AND table_name !='{catalog_table_upper}'""").collect()
    try:
        Parallel(n_jobs=multiprocessing.cpu_count(), backend="threading")(
                delayed(generate_descr)(
                    session,
                    database_name,
                    schema_name,
                    table,
                    catalog_table,
                ) for table in tablenames
            )
        return 'Success'
    except Exception as e:
        # Catch and return the error message
        return f"An error occurred: {str(e)}"
$$;


