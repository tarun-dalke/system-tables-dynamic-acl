# Databricks notebook source
# DBTITLE 1,Remove all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Define the parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("system_catalog", "system")
dbutils.widgets.text("bu_system_catalog", "system_bu")
dbutils.widgets.text("bu_system_catalog_location", "s3://bucket")
dbutils.widgets.text("mapping_table_catalog", "central_platform_config")
dbutils.widgets.text("mapping_table_schema", "system_tables_config")
dbutils.widgets.text("mapping_table_name", "uc_workspace_to_identity_access_mapping")
dbutils.widgets.text("custom_mapping_table_name", "custom_workspace_to_identity_access_mapping")
dbutils.widgets.text("bu_system_catalog_owner", "user@domain.com")
#dbutils.widgets.text("exclude_schemas", "{'__internal_logging','hms_to_uc_migration','access','ai','lineage','storage'}")
dbutils.widgets.text("exclude_schemas", "{'__internal_logging'}")

# COMMAND ----------

# DBTITLE 1,Retrive the parameters
system_catalog = dbutils.widgets.get("system_catalog")
bu_system_catalog = dbutils.widgets.get("bu_system_catalog")
bu_system_catalog_location = dbutils.widgets.get("bu_system_catalog_location")
mapping_table_catalog = dbutils.widgets.get("mapping_table_catalog")
mapping_table_schema = dbutils.widgets.get("mapping_table_schema")
mapping_table_name = dbutils.widgets.get("mapping_table_name")
custom_mapping_table_name = dbutils.widgets.get("custom_mapping_table_name")
bu_system_catalog_owner = dbutils.widgets.get("bu_system_catalog_owner")
exclude_schemas = dbutils.widgets.get("exclude_schemas")
exclude_schemas=eval(exclude_schemas) # to convert String to Set

# COMMAND ----------

#spark.sql(f"""DROP CATALOG {bu_system_catalog}  CASCADE """)


# COMMAND ----------

# DBTITLE 1,Create Restricted Catalog for system tables
spark.sql(f"""
           CREATE CATALOG IF NOT EXISTS  {bu_system_catalog}
              MANAGED LOCATION '{bu_system_catalog_location}' 
              COMMENT 'Catalog for exposing BU specific system tables data to Business Units';
          """)

spark.sql(f""" GRANT ALL PRIVILEGES ON catalog {bu_system_catalog} TO `tarun.dalke@databricks.com` """)
spark.sql(f"""GRANT USE_CATALOG ON CATALOG {bu_system_catalog} TO `account users`;""")
spark.sql(f"""GRANT USE_SCHEMA ON CATALOG {bu_system_catalog} TO `account users`;""")
spark.sql(f"""GRANT SELECT ON catalog {bu_system_catalog} TO `account users`;""")
spark.sql(f"""ALTER catalog {bu_system_catalog} OWNER TO `{bu_system_catalog_owner}`""")


# COMMAND ----------

# DBTITLE 1,Common Functions
def has_workspace_id(absolute_table_name):
  """
    Checks if the specified Table contains a column named 'workspace_id'.

    Parameters:
    - absolute_table_name (str): The absolute name of the Spark table to check - ex. - catalog.schema.table.

    Returns:
    - bool: True if the Table contains a 'workspace_id' column, False otherwise.
  """
  df = spark.table(absolute_table_name)
  if 'workspace_id' in df.columns:
    return True
  else:
    return False
#print(has_workspace_id('system.access.audit'))

#####################################################################################

def get_schema_and_table_names(catalog_name, exclude_schemas: set):
    """
    Retrieves distinct schema and table names from a specified catalog, excluding certain schemas.

    Parameters:
    - catalog_name (str): The name of the catalog to retrieve schema and table names from.
    - exclude_schemas (set): A set containing schema names to exclude from the results.

    Returns:
    - set: A set containing tuples of (schema_name, table_name) for the retrieved schema and table names,
           excluding those specified in 'exclude_schemas'.
    """
    df = spark.sql(f'''
                   SELECT DISTINCT table_schema, table_name 
                   FROM {catalog_name}.information_schema.tables
                   WHERE table_catalog = 'system' AND table_schema NOT IN ('information_schema', 'default')
                   
                   '''
                  )

    schema_tables = {(schema, table) for schema, table in df.collect() if schema not in exclude_schemas}

    return schema_tables
#print(schema_and_table_names('system', {'hms_to_uc_migration', 'compute', 'marketplace', 'storage', 'query','billing'}))

#####################################################################################
def create_schemas(catalog_name:str,schema_names:set) -> None:
  """
    Creates schemas in a specified catalog if they do not already exist.

    Parameters:
    - catalog_name (str): The name of the catalog where the schemas will be created.
    - schema_names (set): A set containing the names of the schemas to be created.

    Returns:
    - None: This function does not return any value.
    """
  for schema_name in schema_names:
    spark.sql(f"""create schema if not exists {catalog_name}.{schema_name}""")

#####################################################################################

def create_restricted_view(source_catalog_name: str, destination_catalog: str, schema_name: str, 
                           table_name: str, mapping_table_catalog: str, mapping_table_schema: str, 
                           mapping_table_name: str) -> None:
    """
    Creates or replaces a restricted view in the destination catalog based on the source table and mapping table.

    Parameters:
    - source_catalog_name (str): The name of the source catalog where the source table resides.
    - destination_catalog (str): The name of the destination catalog where the view will be created.
    - schema_name (str): The schema name in the destination catalog where the view will be created.
    - table_name (str): The name of the source table.
    - mapping_table_catalog (str): The name of the catalog where the mapping table resides.
    - mapping_table_schema (str): The schema name where the mapping table resides.
    - mapping_table_name (str): The name of the mapping table.

    Returns:
    - None: This function does not return any value.
    """

    spark.sql(f"""
              CREATE OR REPLACE VIEW {destination_catalog}.{schema_name}.v_{table_name} AS
                  SELECT ws_name.workspace_name, sys.*
                  FROM {source_catalog_name}.{schema_name}.{table_name} sys
                  LEFT OUTER JOIN 
                    (select  workspace_name,workspace_id from 
                        (select  workspace_name,workspace_id,
                            row_number() over (partition by workspace_id order by creation_time desc) as rn
                        from {mapping_table_catalog}.{mapping_table_schema}.workspaces_detail
                        )
                    WHERE RN=1 
                     )  ws_name
                  ON sys.workspace_id = ws_name.workspace_id
                  WHERE EXISTS (
                                SELECT 1 FROM 
                                    (select WORKSPACE_ID,identity_name
                                    FROM {mapping_table_catalog}.{mapping_table_schema}.{mapping_table_name}
                                    UNION ALL
                                    select WORKSPACE_ID,identity_name
                                    FROM {mapping_table_catalog}.{mapping_table_schema}.{custom_mapping_table_name}
                                    ) AS map 
                                WHERE map.WORKSPACE_id = SYS.WORKSPACE_ID
                                AND (IS_ACCOUNT_GROUP_MEMBER(identity_name) = true or identity_name = current_user() 
                                      --or IS_ACCOUNT_GROUP_MEMBER('admins') = true 
                                      )
                                );
            """)

#####################################################################################
def create_view(source_catalog_name: str, destination_catalog: str, schema_name: str, table_name: str) -> None:
    """
    Creates or replaces a view in the destination catalog based on the source table.

    Parameters:
    - source_catalog_name (str): The name of the source catalog where the source table resides.
    - destination_catalog (str): The name of the destination catalog where the view will be created.
    - schema_name (str): The schema name in the destination catalog where the view will be created.
    - table_name (str): The name of the source table.

    Returns:
    - None: This function does not return any value.
    """
    spark.sql(f"""
              CREATE OR REPLACE VIEW {destination_catalog}.{schema_name}.v_{table_name} AS
                  SELECT *
                  FROM {source_catalog_name}.{schema_name}.{table_name} 
              ;
              """)
#####################################################################################


# COMMAND ----------

# DBTITLE 1,Main Function
def system_catalog_to_bu_system_catalog(system_catalog: str, bu_system_catalog: str, 
                                               mapping_table_catalog: str, mapping_table_schema: str, 
                                               mapping_table_name: str, exclude_schemas: set):
    """
    Create views from the system catalog to the business unit (BU) system catalog.

    Parameters:
    - system_catalog (str): The name of the system catalog containing the source tables.
    - bu_system_catalog (str): The name of the catalog where the tables will be migrated.
    - mapping_table_catalog (str): The name of the catalog containing the mapping table.
    - mapping_table_schema (str): The schema name where the mapping table resides.
    - mapping_table_name (str): The name of the mapping table.
    - exclude_schemas (set): A set containing schema names to exclude from migration.

    Returns:
    - None: This function does not return any value.
    """
    # 1. Get a set of tuple(Schema name, Table name) of all the schemas and tables in System Catalog
    schema_tables = get_schema_and_table_names(catalog_name=system_catalog, exclude_schemas=exclude_schemas)
    print(schema_tables)
    
    # 2. Get a set of all the schemas in System Catalog
    schemas = {schema for schema, table in schema_tables}
    
    # 3. Create all the schemas in bu_system_catalog
    create_schemas(catalog_name=bu_system_catalog, schema_names=schemas)
    
    # 4. For each table in system catalog, create a view in bu_system_catalog
    for schema, table in schema_tables:
        print(f"Createing a view for: {schema}.{table}")
        if has_workspace_id(f"{system_catalog}.{schema}.{table}"):
            
            create_restricted_view(source_catalog_name=system_catalog,
                                   destination_catalog=bu_system_catalog,
                                   schema_name=schema,
                                   table_name=table,
                                   mapping_table_catalog=mapping_table_catalog,
                                   mapping_table_schema=mapping_table_schema,
                                   mapping_table_name=mapping_table_name)
        else:
            create_view(source_catalog_name=system_catalog, destination_catalog=bu_system_catalog,
                        schema_name=schema, table_name=table)
        print(f"View creation completed for : {schema}.{table}")

# COMMAND ----------

# DBTITLE 1,Trigger the main function
system_catalog_to_bu_system_catalog(system_catalog=system_catalog, 
                                    bu_system_catalog=bu_system_catalog, 
                                    mapping_table_catalog=mapping_table_catalog, mapping_table_schema=mapping_table_schema, 
                                    mapping_table_name=mapping_table_name, 
                                    exclude_schemas=exclude_schemas)

# COMMAND ----------

print(exclude_schemas)

