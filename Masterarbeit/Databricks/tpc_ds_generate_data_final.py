#Generating data at larger scales can take hours to run, and you may want to run the notebook as a job.
#
#The cell below generates the data. Read the code carefully, as it contains many parameters to control the process. See the Databricks spark-sql-perf repository README for more information.

#cell1

%scala
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

// Set:
val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
val scaleFactoryInt = scaleFactor.toInt

val scaleName = if(scaleFactoryInt < 1000){
    f"${scaleFactoryInt}%03d" + "GB"
  } else {
    f"${scaleFactoryInt / 1000}%03d" + "TB"
  }

val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}"
val databaseName = "tpcds" + scaleName // name of database to create.

// Run:
val tables = new TPCDSTables(sqlContext,
    dsdgenDir = "/mnt/tpcds-kit/tools", // location of dsdgen
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType

tables.genData(
    location = rootDir,
    format = fileFormat,
    overwrite = true, // overwrite the data that is already there
    partitionTables = false, // create the partitioned fact tables 
    clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = 4) // how many dsdgen partitions to run - number of input tasks.

// Create the specified database
sql(s"create database $databaseName")

// Create the specified database
sql(s"create database $databaseName")

// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(rootDir, fileFormat, databaseName, overwrite = true, discoverPartitions = true)

// Or, if you want to create temporary tables
// tables.createTemporaryTables(location, fileFormat)

// For Cost-based optimizer (CBO) only, gather statistics on all columns:
tables.analyzeTables(databaseName, analyzeColumns = true)

#cell2
# examine data
df = spark.read.parquet("/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/customer")
row_count = df.count()
print("Number of rows:", row_count)

#display(df)

#cell3
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Write to Azure Blob Storage") \
    .getOrCreate()

# Load the DataFrame
df = spark.read.parquet("/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/call_center")

# Define the Azure Blob Storage credentials
storage_account_name = "asdf89762345"
container_name = "qweri"
output_path = "test"
access_key = "cGUZWtrFNSHSKSjs65h6Pp4ep1LkNwq2hlu/YJp2yRDUxS+DGf0cNR06zWAdsukc+ldLtnlOJTqV+AStXmQ4Vw=="

# Write the DataFrame to Azure Blob Storage as Parquet
df.write \
    .format("parquet") \
    .option("header", "true") \
    .save("wasbs://{}/{}{}?{}".format(container_name, storage_account_name, output_path, "fs.azure.account.key=" + access_key))

# Stop the Spark session
spark.stop()

#cell4
jdbc_url = "jdbc:mysql://<hostname>:<port>/<database>"
connection_properties = {
    "user": "<username>",
    "password": "<password>",
    "driver": "com.mysql.jdbc.Driver"
}

# Example connection properties
hostname = "mafuehles.mysql.database.azure.com"
port = "3306"  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Create JDBC URL
jdbc_url = f"jdbc:mysql://{hostname}:{port}/{database}"

# Define connection properties
connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.mysql.jdbc.Driver"
}

#cell5
%sh
pip install mysql-connector-python
pip install pandas
pip install sqlalchemy
pip install pyarrow
pip install time
pip install requests
pip install aiomysql
pip instsall asyncio

#cell6
import mysql.connector

# Example connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Create a connection to the MySQL database
try:
    connection = mysql.connector.connect(
        host=hostname,
        port=port,
        user=username,
        password=password,
        database=database
    )
    print("Connection successful!")
    # Close the connection
    connection.close()
except mysql.connector.Error as error:
    print(f"Error connecting to MySQL database: {error}")

#cell7
import pandas as pd
from sqlalchemy import create_engine

# Example connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Create SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database}?autocommit=True")

# Read Parquet file into DataFrame
parquet_file_path = "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/customer"
df_spark = spark.read.parquet(parquet_file_path)

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_spark.toPandas()

# Write DataFrame to MySQL using SQLAlchemy
table_name = "customer"
chunksize = 1000  # Number of rows to write at a time
for i in range(0, len(df_pandas), chunksize):
    df_pandas.iloc[i:i+chunksize].to_sql(name=table_name, con=engine, if_exists='append', index=False)

# Dispose the engine
engine.dispose()

print("Data written to MySQL successfully.")

#cell8
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Parallel Transfer Parquet Files to MySQL") \
    .getOrCreate()

# Example connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Single table and its corresponding Parquet file
single_table = {"customer_address": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/customer_address"}

# Chunk size for writing data to MySQL
chunksize = 5000  # Adjust as needed

# Function to upload data for a single table
def upload_table(table_name, file_path):
    # Read Parquet file into DataFrame
    df_spark = spark.read.parquet(file_path)

    # Write DataFrame to MySQL using JDBC connector with SSL/TLS encryption
    df_spark.write.format("jdbc") \
        .option("url", f"jdbc:mysql://{hostname}:{port}/{database}?useSSL=true&requireSSL=true") \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .option("rewriteBatchedStatements", "true") \
        .option("batchsize", chunksize) \
        .mode("append") \
        .save()

# Maximum number of threads for parallel execution
max_threads = 4  # Adjust as needed

# Create a thread pool executor
with ThreadPoolExecutor(max_workers=max_threads) as executor:
    # Submit the task for the single table upload
    future = executor.submit(upload_table, list(single_table.keys())[0], list(single_table.values())[0])

# Wait for the task to complete
future.result()

# Stop SparkSession
spark.stop()

print("Data written to MySQL successfully.")

# Function to download SQL files
def download_sql(url):
    response = requests.get(url)
    response.raise_for_status()  # will throw an error for bad status
    return response.text

# MySQL connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# GitHub base URL
base_url = "https://raw.githubusercontent.com/Agirish/tpcds/master/"

# Define the batch size
batch_size = 1

# Track the execution time and success of each query
execution_times = []
success_flags = []

# Create a connection to the MySQL database
connection = mysql.connector.connect(
    host=hostname,
    port=port,
    user=username,
    password=password,
    database=database
)
cursor = connection.cursor()

# Iterate over query files
for i in range(1, 2):
    sql_file_url = f"{base_url}query{i}.sql"
    try:
        # Download SQL query
        sql_query = download_sql(sql_file_url)
        print(f"Downloaded query {i}.sql successfully")

        # Execute the query
        start_time = time.time()
        cursor.execute(sql_query)
        connection.commit()
        print(f"Executed query {i}.sql successfully")

        # Record success and execution time
        success_flags.append(True)
        execution_times.append(time.time() - start_time)
    except Exception as e:
        print(f"Failed to execute query {i}.sql: {str(e)}")
        success_flags.append(False)
        execution_times.append(None)

# Close the cursor and connection
cursor.close()
connection.close()

# Output the execution times and success flags
for idx, (execution_time, success) in enumerate(zip(execution_times, success_flags)):
    print(f"Query {idx + 1} Execution Time: {execution_time:.2f} seconds, Success: {success}")

# Store execution times to a file
with open("/mnt/datalake/raw/tpc-ds/query_execution_times.csv", "w") as f:
    f.write("Query Index,Execution Time (seconds),Success\n")
    for idx, (execution_time, success) in enumerate(zip(execution_times, success_flags)):
        f.write(f"{idx + 1},{execution_time if execution_time is not None else 'N/A'},{success}\n")

#cell16
import requests
import mysql.connector
import time

# Function to download SQL files
def download_sql(url):
    response = requests.get(url)
    response.raise_for_status()  # will throw an error for bad status
    return response.text

# MySQL connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# GitHub base URL
base_url = "https://raw.githubusercontent.com/Agirish/tpcds/master/"

# Define the batch size
batch_size = 1

# Track the execution time and success of each query
execution_times = []
success_flags = []

# Create a connection to the MySQL database
connection = mysql.connector.connect(
    host=hostname,
    port=port,
    user=username,
    password=password,
    database=database
)
cursor = connection.cursor()

# Iterate over query files
big_sql_query = ""
for i in range(1, 3):
    sql_file_url = f"{base_url}query{i}.sql"
    try:
        # Download SQL query
        sql_query = download_sql(sql_file_url)
        print(f"Downloaded query {i}.sql successfully")

        # Append downloaded SQL query to big SQL query
        big_sql_query += sql_query.strip() + "\n\n"
    except Exception as e:
        print(f"Failed to download query {i}.sql: {str(e)}")

# Store big SQL query to a file
big_sql_file_path = "/mnt/datalake/raw/testi.sql"  # Replace with the desired file path
dbutils.fs.put(big_sql_file_path, big_sql_query, overwrite=True)


print("Big SQL file created successfully.")

# Close the cursor and connection
cursor.close()
connection.close()

#cell17
import shutil

# Define the source and destination paths
source_path = "/Workspace/Users/tobiasfuehles@aol.com/test/sql_files/test.sql"
destination_directory = "/mnt/qweri/"

# Move the file to the destination directory
shutil.move(source_path, destination_directory)

#cell19
import mysql.connector
from pyspark.dbutils import DBUtils

# MySQL connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Connect to the MySQL database
connection = mysql.connector.connect(
    host=hostname,
    port=port,
    user=username,
    password=password,
    database=database
)
cursor = connection.cursor()

# Set profiling to 1
cursor.execute("SET profiling = 1;")

# Get the big SQL query
big_sql_file_path = "/mnt/datalake/raw/testi.sql"  # Replace with the correct path
big_sql_query = dbutils.fs.head(big_sql_file_path)

# Execute the big SQL query
cursor.execute(big_sql_query)

# Discard the result set
cursor.fetchall()

connection.commit()

# Get the profiling results
cursor.execute("SHOW PROFILES")
profiles = cursor.fetchall()

# Save profiling results to a file
profiling_results_path = "/mnt/datalake/raw/profiling_results.txt"  # Replace with the desired file path
with open(profiling_results_path, "w") as result_file:
    for profile in profiles:
        result_file.write(str(profile) + "\n")

# Close the cursor and connection
cursor.close()
connection.close()