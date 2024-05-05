#Generating TPC-DS datasets with spark-sql-perf involves the following steps.
#
#
#Add the spark-sql-perf library jar to your Databricks cluster.
#Mount Azure Data Lake Gen2
#Install the Databricks TPC-DS benchmark kit.
#Restart the cluster.
#Run the TPC-DS-Generate notebook to generate the data and setup the database.
#
#
#Install the jar file from the BlueGranite tpc-ds-dataset-generator repo using the Databricks cluster Libraries menu. Use the jar file that matches the Scala version of the cluster. When downloading the file, make sure you click through to the jar file page in the repo and use the Download button to get the actual file.
#
#Alternately you can build the spark-sql-perf library jar yourself using sbt:
#
#
#Install sbt on your local machine using the instructions here.
#Clone the spark-sql-perf repository and navigate to the new directory.
#
#git clone https://github.com/databricks/spark-sql-perf.git
#
#Run sbt package to build for scala 2.11. Run sbt +package to build for scala 2.11 and 2.12.
#The jar file can then be found in the /spark-sql-perf/target/scala-2.1x directory. Install the library using the Databricks cluster Libraries menu.

#cell1
# Define required configurations
clientId = "b13066a3-71e8-4b12-a1cd-e888b7d658ec"
clientSecret = "DB18Q~jUsFPC4mcH09yoJ8mOmdVU4jnJrp8wGcmN"
tenantId = "23aa4c02-d2f6-4ee1-a93c-ec2eb7540427"
storageAccountName = "asdf89762345"
fileSystemName = "qweri"  # Container name

# Define mount point directory
mountPoint = f"/mnt/{fileSystemName}"

# Define configurations
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": clientId,
    "fs.azure.account.oauth2.client.secret": clientSecret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantId}/oauth2/token"
}

# Determine if mount point already exists
mount_exists = False
for m in dbutils.fs.mounts():
    if m.mountPoint == mountPoint:
        mount_exists = True
        print(f"Mount point {mountPoint} already exists. Unmounting...")
        dbutils.fs.unmount(mountPoint)
        break

# Create mount point directory if it doesn't exist
if not mount_exists:
    print(f"Creating mount point directory {mountPoint}")
    dbutils.fs.mkdirs(mountPoint)

# Check if large file shares are disabled
large_file_shares_enabled = True

# Create mount if large file shares are enabled
if large_file_shares_enabled:
    # Create mount
    print(f"Creating mount point {mountPoint}")
    dbutils.fs.mount(
        source=f"abfss://{fileSystemName}@{storageAccountName}.dfs.core.windows.net/",
        mount_point=mountPoint,
        extra_configs=configs
    )
else:
    print("Large file shares are disabled. Mounting is not supported.")

#cell2
# Load the file from mounted storage into Databricks
# Load the file from mounted storage into Databricks
df = spark.read.parquet("/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/customer")

output_path = f"{mountPoint}/customer1.parquet"

# Write the DataFrame to blob storage as Parquet
df.write.parquet(output_path)

#cell3
# Assuming you've already mounted Azure Blob Storage to Databricks
# You can mount it using the following command:
# dbutils.fs.mount(source="wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/", mount_point="/mnt/<mount_name>", extra_configs=configs)

# Read data from Azure Blob Storage
df = spark.read.parquet("/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/customer")

# Configure Hive metastore URI with username and password
hive_metastore_uri = "thrift://admin:12Compexpert1994-@testilovici.azurehdinsisght.net:10000"

# Configure Hive database and table name
hive_database = "ma"
hive_table = "customer"

# Write DataFrame to Hive table
df.write.mode("overwrite").format("parquet").saveAsTable(f"{hive_database}.{hive_table}")

# Optionally, you can run some queries to verify that the data has been successfully written
# For example, you can count the number of rows in the table:
spark.sql(f"SELECT COUNT(*) FROM {hive_database}.{hive_table}").show()

#cell4

import pyodbc

# Set up connection parameters
hive_metastore_uri = "thrift://admin:12Compexpert1994-@testilovici.azurehdinsisght.net:10000"
username = "admin"
password = "12Compexpert1994-"

# Establish connection
conn_str = f"DSN=HiveMetastore;Host={hive_metastore_uri};UID={username};PWD={password}"
conn = pyodbc.connect(conn_str)

# Execute a query to list databases
cursor = conn.cursor()
cursor.execute("SHOW DATABASES")

# Fetch and print results
for row in cursor.fetchall():
    print(row)

#cell5
# Define the source and destination paths
source_path = "/sql_files/test.sql"
destination_directory = "/mnt/datalake/raw/"

# Move the file to the destination directory using dbutils.fs.mv()
dbutils.fs.mv(source_path, destination_directory)

#cell6
# Define the directory path where you want to store the init script
directory_path = "/dbfs/Volumes/"

# Define the init script file name
script_name = "tpcds-install.sh"

# Define the script content
script_content = """
#!/bin/bash
sudo apt-get --assume-yes install gcc make flex bison byacc git

cd /usr/local/bin
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX
"""

# Define the full path to the init script file
script_path = directory_path + script_name

# Upload the init script to the specified directory
dbutils.fs.put(script_path, script_content, True)

#cell7
# Check if the file exists
file_path = "/dbfs/Volumes/tpcds-install.sh"

if dbutils.fs.ls(file_path):
    print(f"The file {file_path} exists.")
else:
    print(f"The file {file_path} does not exist.")

#Add the init script to your cluster using the steps below:
#
#
#On the cluster configuration page, click Advanced Options.
#At the bottom of the page, click Init Scripts.
#
#Databricks Init Script menu
#
#In the Destination drop-down, select a destination type.
#Specify a path to the init script.
#Click Add.
#Upload your script to the specified location.
#Restart the cluster.