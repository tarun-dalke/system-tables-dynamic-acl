# Databricks notebook source
# DBTITLE 1,Install Databricks SDK
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Remove Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Define Parameters
## Make sure to set "accounts.cloud.databricks.com", "accounts.azuredatabricks.net" or "accounts.gcp.databricks.com" as appropriate.
dbutils.widgets.text("host", "accounts.cloud.databricks.com")

# Current account_id, ie 7a99b43c-b46c-432b-b0a7-1234567
dbutils.widgets.text("account_id", "")

# CLIENT_ID and CLIENT_SECRET come from the account-admin service principal, if you don't have one, follow the Step 1 of this doc:
# https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html#step-1-create-a-service-principal
# Once you have the service principal ready, generate a CLIENT_SECRET from the Step 3 in the doc mentioned above.
dbutils.widgets.text("client_id", "")

# Ideally, don't store the secret in a raw string form, use SECRETS
# https://docs.databricks.com/en/security/secrets/index.html
dbutils.widgets.text("client_secret", "")

dbutils.widgets.text("workspaces_detail_table_name", "central_platform_config.system_tables_config.workspaces_detail")

dbutils.widgets.text("workspaces_identity_table_name", "central_platform_config.system_tables_config.uc_workspace_to_identity_access_mapping")


# COMMAND ----------

# DBTITLE 1,Retrive the parameters
HOST = dbutils.widgets.get("host")
ACCOUNT_ID = dbutils.widgets.get("account_id")
CLIENT_ID = dbutils.widgets.get("client_id")
CLIENT_SECRET = dbutils.widgets.get("client_secret")
WORKSPACES_DETAIL_TABLE_NAME = dbutils.widgets.get("workspaces_detail_table_name")
WORKSPACES_IDENTITY_TABLE_NAME = dbutils.widgets.get("workspaces_identity_table_name")



# COMMAND ----------

# DBTITLE 1,Cretae Databricks SDK Account Client
import re

from databricks.sdk import AccountClient

from pyspark.sql.functions import *


a = AccountClient(
        host=HOST,
        account_id=ACCOUNT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )


# COMMAND ----------

# DBTITLE 1,Schema defination of workspaces detail table
from delta.tables import *
from pyspark.sql.types import *

WORKSPACES_IDENTITY_MAPPING_SCHEMA = StructType(
    [
        StructField("workspace_id", LongType(), True),
        StructField("workspace_name", StringType(), True),
        StructField("identity_name", StringType(), True),
        StructField("identity_type", StringType(), True),
        StructField("permissions", ArrayType(StringType(),True)),
        StructField("display_name", StringType(), True)
        
        # TODO - extend as needed
    ]
)


WORKSPACES_TABLE_SCHEMA_AWS = StructType(
    [
        StructField("account_id", StringType(), True),
        StructField("aws_region", StringType(), True),
        StructField("creation_time", LongType(), True),
        StructField("credentials_id", StringType(), True),
        StructField("deployment_name", StringType(), True),
        StructField("managed_services_customer_managed_key_id", StringType(), True),
        StructField("network_id", StringType(), True),
        StructField("pricing_tier", StringType(), True),  # Enum converted to String
        StructField("private_access_settings_id", StringType(), True),
        StructField("storage_configuration_id", StringType(), True),
        StructField("storage_customer_managed_key_id", StringType(), True),
        StructField("workspace_id", LongType(), True),
        StructField("workspace_name", StringType(), True),
        StructField("workspace_status", StringType(), True),  # Enum converted to String
        StructField("workspace_status_message", StringType(), True),
        # TODO - extend as needed
    ]
)
WORKSPACES_TABLE_SCHEMA_AZURE =  StructType(
    [
        StructField("account_id", StringType(), True),
        StructField(
            "azure_workspace_info", StringType(), True
        ),  # Assuming StringType for simplicity, adjust as needed
        StructField("creation_time", LongType(), True),
        StructField("deployment_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("pricing_tier", StringType(), True),  # Enum converted to String
        StructField("workspace_id", LongType(), True),
        StructField("workspace_name", StringType(), True),
        StructField("workspace_status", StringType(), True),  # Enum converted to String
        StructField("workspace_status_message", StringType(), True),
        # TODO - extend as needed
    ]
)

if print(a.config.is_azure):
    WORKSPACES_TABLE_SCHEMA = WORKSPACES_TABLE_SCHEMA_AZURE
else:
    WORKSPACES_TABLE_SCHEMA = WORKSPACES_TABLE_SCHEMA_AWS

# COMMAND ----------

# DBTITLE 1,Common Functions
def save_as_table(table_path, schema, df, pk_columns=["id"]):
    assert (
        df.schema == schema
    ), f"""
      Schemas are not equal.
      Expected: {schema}
      Actual: {df.schema}"""

    deltaTable = (
        DeltaTable.createIfNotExists(spark)
        .tableName(table_path)
        .addColumns(schema)
        .execute()
    )

    merge_statement = " AND ".join([f"logs.{col}=newLogs.{col}" for col in pk_columns])

    (
        deltaTable.alias("logs")
        .merge(
            df.alias("newLogs"),
            f"{merge_statement}",
        )
        .whenNotMatchedInsertAll()
        .whenMatchedUpdateAll()
        .execute()
    )

def overwrite_table(table_path, schema, df):
    assert (
        df.schema == schema
    ), f"""
      Schemas are not equal.
      Expected: {schema}
      Actual: {df.schema}"""

    deltaTable = (
        DeltaTable.createIfNotExists(spark)
        .tableName(table_path)
        .addColumns(schema)
        .execute()
    )

    df.write.mode("overwrite").saveAsTable(f"{table_path}")


# COMMAND ----------

# DBTITLE 1,Common functions
from pyspark.sql.functions import lit
from functools import reduce,partial
from pyspark.sql import DataFrame


def create_workspace_id_name_mapping_table(workspaces,WORKSPACES_DETAIL_TABLE_NAME,WORKSPACES_TABLE_SCHEMA):
    """
    Creates a mapping table of workspace IDs and names.

    This function fetches workspace details from the Account API, converts the data into a Spark DataFrame,
    and saves it as a table with the specified name and schema.

    Parameters:
    WORKSPACES_DETAIL_TABLE_NAME (str): The name of the table to be created.
    WORKSPACES_TABLE_SCHEMA (StructType): The schema of the table to be created.

    Returns:
    None

    Side Effects:
    - Prints messages indicating the progress of the operation.
    - Creates a table in the database with workspace details.

    Example Usage:
    >>> WORKSPACES_DETAIL_TABLE_NAME = "workspace_details"
    >>> WORKSPACES_TABLE_SCHEMA = StructType([
    >>>     StructField("workspace_id", StringType(), False),
    >>>     StructField("workspace_name", StringType(), True)
    >>> ])
    >>> create_workspace_id_name_mapping_table(WORKSPACES_DETAIL_TABLE_NAME, WORKSPACES_TABLE_SCHEMA)
    Fetching workspaces from the Account API..
    workspace_details created (10 rows)
    """
    
    #print("Fetching workspaces from the Account API..")
    #workspaces = [workspace.as_dict() for workspace in a.workspaces.list()]
    workspaces_df = spark.createDataFrame(workspaces, schema=WORKSPACES_TABLE_SCHEMA)

    
    save_as_table(
        WORKSPACES_DETAIL_TABLE_NAME,
        WORKSPACES_TABLE_SCHEMA,
        workspaces_df,
        pk_columns=["workspace_id"],
    )
    
    print(f"{WORKSPACES_DETAIL_TABLE_NAME} created ({len(workspaces)} rows)")


def create_workspace_identity_name_mapping_table(workspaces,WORKSPACES_IDENTITY_TABLE_NAME,WORKSPACES_IDENTITY_MAPPING_SCHEMA):
    """
    Creates a mapping table of workspace IDs and names.

    This function fetches workspace details from the Account API, converts the data into a Spark DataFrame,
    and saves it as a table with the specified name and schema.

    Parameters:
    workspaces (list) : List of workspaces
    WORKSPACES_DETAIL_TABLE_NAME (str): The name of the table to be created.
    WORKSPACES_TABLE_SCHEMA (StructType): The schema of the table to be created.

    Returns:
    None

    Side Effects:
    - Prints messages indicating the progress of the operation.
    - Creates a table in the database with workspace details.

    Example Usage:
    >>> workspaces = "[]"
    >>> WORKSPACES_DETAIL_TABLE_NAME = "workspace_details"
    >>> WORKSPACES_TABLE_SCHEMA = StructType([
    >>>     StructField("workspace_id", StringType(), False),
    >>>     StructField("workspace_name", StringType(), True)
    >>> ])
    >>> create_workspace_id_name_mapping_table(WORKSPACES_DETAIL_TABLE_NAME, WORKSPACES_TABLE_SCHEMA)
    Fetching workspaces from the Account API..
    workspace_details created (10 rows)
    """
    
    workspace_identity_df_list = []
    #print("Fetching workspaces from the Account API..")
    #workspaces = [workspace.as_dict() for workspace in a.workspaces.list()]
    workspaces_df = spark.createDataFrame(workspaces)

    workspaces = workspaces_df.select("workspace_id","workspace_name").dropDuplicates().collect()
    
    for workspace_id,workspace_name in workspaces:
        try :
            Assignments = [identities.as_dict() for identities in a.workspace_assignment.list(workspace_id)]
            Assignments= [{"workspace_id": workspace_id,"workspace_name": workspace_name,**i} for i in Assignments]

        except Exception as exception:
            print(f"Workspace {workspace_id} is not UC enabled. Exception : {exception}")
            
            assert str(exception) =='Permission assignment APIs are not available for this workspace.'

        try :
            Assignments_df = spark.createDataFrame(Assignments)

        except Exception as exception:
            print(exception)

        workspace_identity_df_list.append(Assignments_df)

    unified_workspace_identity_df=reduce(DataFrame.unionAll,workspace_identity_df_list)
    unified_workspace_identity_df.createOrReplaceTempView("workspace_identity_Assignments")
        
    complete_assignments_df = spark.sql(
    """select  workspace_id,workspace_name,
                case when principal.user_name is not null then principal.user_name 
                                 when principal.service_principal_name is not null then principal.service_principal_name
                                 when principal.group_name is not null then principal.group_name
                                 else null end as identity_name,
                            case when principal.user_name is not null then "user" 
                                 when principal.service_principal_name is not null then "service_principal"
                                 when principal.group_name is not null then "group"
                                 else null end as identity_type,
                permissions,
                principal.display_name as display_name


        from workspace_identity_Assignments
    """).drop_duplicates()
    
    overwrite_table(WORKSPACES_IDENTITY_TABLE_NAME, WORKSPACES_IDENTITY_MAPPING_SCHEMA, complete_assignments_df)



# COMMAND ----------

print("Fetching workspaces from the Account API..")
workspaces = [workspace.as_dict() for workspace in a.workspaces.list()]
print("****  Creating workspace id to workspace name mapping table  ****")
create_workspace_id_name_mapping_table(workspaces,WORKSPACES_DETAIL_TABLE_NAME,WORKSPACES_TABLE_SCHEMA)

print("****  Creating workspace id to Identity name mapping table  ****")
create_workspace_identity_name_mapping_table(workspaces,WORKSPACES_IDENTITY_TABLE_NAME,WORKSPACES_IDENTITY_MAPPING_SCHEMA)
