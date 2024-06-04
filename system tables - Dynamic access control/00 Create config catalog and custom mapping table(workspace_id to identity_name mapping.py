# Databricks notebook source
# DBTITLE 1,Remove All Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Define the parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("system_catalog", "system")
dbutils.widgets.text("mapping_table_catalog", "central_platform_config")
dbutils.widgets.text("mapping_table_catalog_location", "s3://bucket/")
dbutils.widgets.text("mapping_table_schema", "system_tables_config")
dbutils.widgets.text("mapping_table_name", "custom_workspace_to_identity_access_mapping")
dbutils.widgets.text("mapping_table_catalog_owner", "user@domain.com")

# COMMAND ----------

# DBTITLE 1,Retrive the parameters
system_catalog = dbutils.widgets.get("system_catalog")
mapping_table_catalog = dbutils.widgets.get("mapping_table_catalog")
mapping_table_catalog_location = dbutils.widgets.get("mapping_table_catalog_location")
mapping_table_schema = dbutils.widgets.get("mapping_table_schema")
mapping_table_name = dbutils.widgets.get("mapping_table_name")
mapping_table_catalog_owner = dbutils.widgets.get("mapping_table_catalog_owner")

# COMMAND ----------

# DBTITLE 1,Create Config catalog if does not exist
spark.sql(f"""
           CREATE CATALOG IF NOT EXISTS  {mapping_table_catalog}
              MANAGED LOCATION '{mapping_table_catalog_location}' 
              COMMENT 'Catalog for Central Platform Configuration Data';
          """)

spark.sql(f""" GRANT ALL PRIVILEGES ON catalog {mapping_table_catalog} TO `user@domain.com` """)
spark.sql(f""" ALTER schema {mapping_table_catalog}.default OWNER TO `{mapping_table_catalog_owner}` """)
spark.sql(f""" ALTER catalog {mapping_table_catalog} OWNER TO `{mapping_table_catalog_owner}` """)




# COMMAND ----------

# DBTITLE 1,Create Config scehma if not exists for stroing mapping table
spark.sql(f"""
          CREATE SCHEMA IF NOT EXISTS {mapping_table_catalog}.{mapping_table_schema} 
          COMMENT 'Schema for Housing System Catalog Access Configuration Data';
          """)

spark.sql(f"""ALTER SCHEMA {mapping_table_catalog}.{mapping_table_schema} OWNER TO `{mapping_table_catalog_owner}`""")

# COMMAND ----------

# DBTITLE 1,Create Mapping Table
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {mapping_table_catalog}.{mapping_table_schema}.{mapping_table_name}
          (
              workspace_id STRING,
              identity_name STRING
          )
          COMMENT 'This table serves as a mapping of workspaces to the groups authorized to access data from system tables, specifically tailored to the business unit (BU) workspaces.'
          --TBLPROPERTIES ('foo'='bar');
          """)

spark.sql(f"""ALTER TABLE {mapping_table_catalog}.{mapping_table_schema}.{mapping_table_name} OWNER TO `{mapping_table_catalog_owner}`""")

# COMMAND ----------

# DBTITLE 1,Create a view on Mapping table with Data Deduplication
spark.sql(f"""
            CREATE OR REPLACE VIEW {mapping_table_catalog}.{mapping_table_schema}.v_{mapping_table_name} AS
            (
              SELECT 
                workspace_id, 
                COLLECT_SET(identity_name) AS identity_name
              FROM ( 
                  SELECT 
                    t.*, 
                    ROW_NUMBER () OVER (PARTITION BY workspace_id, identity_name ORDER BY workspace_id) AS RN
                  FROM {mapping_table_catalog}.{mapping_table_schema}.{mapping_table_name} t
                  )
              WHERE RN = 1
              GROUP BY workspace_id
            );
          """)

# COMMAND ----------

# DBTITLE 1,Add data to mapping table
spark.sql(f"""
          INSERT INTO TABLE {mapping_table_catalog}.{mapping_table_schema}.{mapping_table_name} 
          (workspace_id, identity_name) VALUES
              ('6543211213313', 'humans'),
              ('1234562313131', 'user@domain.com')
              ;
          """)
