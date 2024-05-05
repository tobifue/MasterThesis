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

#cell9
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Transfer Parquet Files to MySQL") \
    .getOrCreate()

# Example connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Dictionary of tables and their corresponding parquet files
table_filess = {
  "catalog_sales": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/catalog_sales",
    "catalog_returns": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/catalog_returns",
    "inventory": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/inventory",
    "store_sales": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/store_sales",
    "store_returns": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/store_returns",
    "web_sales": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/web_sales",
    "web_returns": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/web_returns",
    "call_center": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/call_center",
    "catalog_page": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/catalog_page",
    "customer": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/customer",
    #"customer_address": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/customer_address",
    "customer_demographics": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/customer_demographics",
    "date_dim": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/date_dim",
    "household_demographics": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/household_demographics",
    "income_band": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/income_band",
    "item": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/item",
    "promotion": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/promotion",
    "reason": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/reason",
    "ship_mode": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/ship_mode",
    "store": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/store",
    "time_dim": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/time_dim"
}

table_files = {
  "warehouse": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/warehouse",
    "web_page": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/web_page",
    "web_site": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/web_site"
   
}

single_table ={    "store_sales": "/mnt/datalake/raw/tpc-ds/source_files_001GB_parquet/store_sales"
}


# Chunk size for writing data to MySQL
chunksize = 5000  # Adjust as needed

# Function to upload data for a single table
def upload_table(table_name, file_path):
    # Read parquet file into DataFrame
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
    # Submit tasks for each table upload
    futures = [executor.submit(upload_table, table_name, file_path) for table_name, file_path in table_files.items()]

# Wait for all tasks to complete
for future in futures:
    future.result()

# Stop SparkSession
spark.stop()

print("Data written to MySQL successfully.")

#cell10
import mysql.connector
import time

# Example connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Define the list of queries to execute
queries = [
    """SELECT 
    cd_gender, 
    cd_marital_status, 
    cd_education_status, 
    COUNT(*) AS cnt1, 
    cd_purchase_estimate, 
    COUNT(*) AS cnt2, 
    cd_credit_rating, 
    COUNT(*) AS cnt3, 
    cd_dep_count, 
    COUNT(*) AS cnt4, 
    cd_dep_employed_count, 
    COUNT(*) AS cnt5, 
    cd_dep_college_count, 
    COUNT(*) AS cnt6 
FROM   
    customer c 
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE  
    ca_county IN ( 'Lycoming County', 'Sheridan County', 'Kandiyohi County', 'Pike County', 'Greene County' ) 
    AND EXISTS (
        SELECT * 
        FROM store_sales ss
        JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk 
        WHERE c.c_customer_sk = ss.ss_customer_sk 
        AND d1.d_year = 2002 
        AND d1.d_moy BETWEEN 4 AND 4 + 3
    ) 
    AND (
        EXISTS (
            SELECT * 
            FROM web_sales ws
            JOIN date_dim d2 ON ws.ws_sold_date_sk = d2.d_date_sk 
            WHERE c.c_customer_sk = ws.ws_bill_customer_sk 
            AND d2.d_year = 2002 
            AND d2.d_moy BETWEEN 4 AND 4 + 3
        ) 
        OR EXISTS (
            SELECT * 
            FROM catalog_sales cs
            JOIN date_dim d3 ON cs.cs_sold_date_sk = d3.d_date_sk 
            WHERE c.c_customer_sk = cs.cs_ship_customer_sk 
            AND d3.d_year = 2002 
            AND d3.d_moy BETWEEN 4 AND 4 + 3
        ) 
    )
GROUP  BY 
    cd_gender, 
    cd_marital_status, 
    cd_education_status, 
    cd_purchase_estimate, 
    cd_credit_rating, 
    cd_dep_count, 
    cd_dep_employed_count, 
    cd_dep_college_count 
ORDER  BY 
    cd_gender, 
    cd_marital_status, 
    cd_education_status, 
    cd_purchase_estimate, 
    cd_credit_rating, 
    cd_dep_count, 
    cd_dep_employed_count, 
    cd_dep_college_count
LIMIT 100; """,
    "SELECT count(*) FROM web_site;",
    # Add the remaining queries here
]

# Track the execution time and success of each query
execution_times = []
success_flags = []
for query in queries:
    start_time = time.time()
    try:
        # Create a connection to the MySQL database
        connection = mysql.connector.connect(
            host=hostname,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = connection.cursor()
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch and discard the results
        cursor.fetchall()
        
        # Commit the transaction
        connection.commit()
        
        # Close the cursor and connection
        cursor.close()
        connection.close()
        
        success_flags.append(True)
    except mysql.connector.Error as e:
        print(f"Error executing query: {query}, Error: {str(e)}")
        success_flags.append(False)
    finally:
        execution_times.append(time.time() - start_time)

# Output the execution times and success flags
for idx, (execution_time, success) in enumerate(zip(execution_times, success_flags)):
    print(f"Query {idx + 1} Execution Time: {execution_time:.2f} seconds, Success: {success}")

#cell1
import mysql.connector
import time

# Example connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Define the list of queries to execute
queries = [
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_sm FOREIGN KEY (cr_ship_mode_sk) REFERENCES ship_mode (sm_ship_mode_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_b_hd FOREIGN KEY (cs_bill_hdemo_sk) REFERENCES household_demographics (hd_demo_sk);",
    # Add the remaining queries here
]

# Track the execution time and success of each query
execution_times = []
success_flags = []
for query in queries:
    start_time = time.time()
    try:
        # Create a connection to the MySQL database
        connection = mysql.connector.connect(
            host=hostname,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = connection.cursor()
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch and discard the results
        cursor.fetchall()
        
        # Commit the transaction
        connection.commit()
        
        # Close the cursor and connection
        cursor.close()
        connection.close()
        
        success_flags.append(True)
    except mysql.connector.Error as e:
        print(f"Error executing query: {query}, Error: {str(e)}")
        success_flags.append(False)
    finally:
        execution_times.append(time.time() - start_time)

# Output the execution times and success flags
for idx, (execution_time, success) in enumerate(zip(execution_times, success_flags)):
    print(f"Query {idx + 1} Execution Time: {execution_time:.2f} seconds, Success: {success}")

#cell12
import mysql.connector
import time

# Example connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Define the list of queries to execute
queries = [
   "alter table call_center add constraint cc_d1 foreign key  (cc_closed_date_sk) references date_dim (d_date_sk);",
    "alter table call_center add constraint cc_d2 foreign key  (cc_open_date_sk) references date_dim (d_date_sk);",
    "alter table catalog_page add constraint cp_d1 foreign key  (cp_end_date_sk) references date_dim (d_date_sk);",
    "alter table catalog_page add constraint cp_d2 foreign key  (cp_start_date_sk) references date_dim (d_date_sk);",
    "alter table catalog_returns add constraint cr_cc foreign key  (cr_call_center_sk) references call_center (cc_call_center_sk);",
    "alter table catalog_returns add constraint cr_cp foreign key  (cr_catalog_page_sk) references catalog_page (cp_catalog_page_sk);",
    "alter table catalog_returns add constraint cr_i foreign key  (cr_item_sk) references item (i_item_sk);",
    "alter table catalog_returns add constraint cr_r foreign key  (cr_reason_sk) references reason (r_reason_sk);",
    "alter table catalog_returns add constraint cr_a1 foreign key  (cr_refunded_addr_sk) references customer_address (ca_address_sk);",
    "alter table catalog_returns add constraint cr_cd1 foreign key  (cr_refunded_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table catalog_returns add constraint cr_c1 foreign key  (cr_refunded_customer_sk) references customer (c_customer_sk);",
    "alter table catalog_returns add constraint cr_hd1 foreign key  (cr_refunded_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table catalog_returns add constraint cr_d1 foreign key  (cr_returned_date_sk) references date_dim (d_date_sk);",
    "alter table catalog_returns add constraint cr_t foreign key  (cr_returned_time_sk) references time_dim (t_time_sk);",
    "alter table catalog_returns add constraint cr_a2 foreign key  (cr_returning_addr_sk) references customer_address (ca_address_sk);",
    "alter table catalog_returns add constraint cr_cd2 foreign key  (cr_returning_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table catalog_returns add constraint cr_c2 foreign key  (cr_returning_customer_sk) references customer (c_customer_sk);",
    "alter table catalog_returns add constraint cr_hd2 foreign key  (cr_returning_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table catalog_returns add constraint cr_sm foreign key  (cr_ship_mode_sk) references ship_mode (sm_ship_mode_sk);",
    "alter table catalog_returns add constraint cr_w2 foreign key  (cr_warehouse_sk) references warehouse (w_warehouse_sk);",
    "alter table catalog_sales add constraint cs_b_a foreign key  (cs_bill_addr_sk) references customer_address (ca_address_sk);",
    "alter table catalog_sales add constraint cs_b_cd foreign key  (cs_bill_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table catalog_sales add constraint cs_b_c foreign key  (cs_bill_customer_sk) references customer (c_customer_sk);",
    "alter table catalog_sales add constraint cs_b_hd foreign key  (cs_bill_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table catalog_sales add constraint cs_cc foreign key  (cs_call_center_sk) references call_center (cc_call_center_sk);", 
    "alter table catalog_sales add constraint cs_cp foreign key  (cs_catalog_page_sk) references catalog_page (cp_catalog_page_sk);",
    "alter table catalog_sales add constraint cs_i foreign key  (cs_item_sk) references item (i_item_sk);",
    "alter table catalog_sales add constraint cs_p foreign key  (cs_promo_sk) references promotion (p_promo_sk);",
    "alter table catalog_sales add constraint cs_s_a foreign key  (cs_ship_addr_sk) references customer_address (ca_address_sk);",
    "alter table catalog_sales add constraint cs_s_cd foreign key  (cs_ship_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table catalog_sales add constraint cs_s_c foreign key  (cs_ship_customer_sk) references customer (c_customer_sk);",
    "alter table catalog_sales add constraint cs_d1 foreign key  (cs_ship_date_sk) references date_dim (d_date_sk);",
    "alter table catalog_sales add constraint cs_s_hd foreign key  (cs_ship_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table catalog_sales add constraint cs_sm foreign key  (cs_ship_mode_sk) references ship_mode (sm_ship_mode_sk);",
    "alter table catalog_sales add constraint cs_d2 foreign key  (cs_sold_date_sk) references date_dim (d_date_sk);",
    "alter table catalog_sales add constraint cs_t foreign key  (cs_sold_time_sk) references time_dim (t_time_sk);",
    "alter table catalog_sales add constraint cs_w foreign key  (cs_warehouse_sk) references warehouse (w_warehouse_sk);",
     "alter table customer add constraint c_a foreign key  (c_current_addr_sk) references customer_address (ca_address_sk);",
    "alter table customer add constraint c_cd foreign key  (c_current_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table customer add constraint c_hd foreign key  (c_current_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table customer add constraint c_fsd foreign key  (c_first_sales_date_sk) references date_dim (d_date_sk);",
    "alter table customer add constraint c_fsd2 foreign key  (c_first_shipto_date_sk) references date_dim (d_date_sk);",
    "alter table household_demographics add constraint hd_ib foreign key  (hd_income_band_sk) references income_band (ib_income_band_sk);",
    "alter table inventory add constraint inv_d foreign key  (inv_date_sk) references date_dim (d_date_sk);",
    "alter table inventory add constraint inv_i foreign key  (inv_item_sk) references item (i_item_sk);",
    "alter table inventory add constraint inv_w foreign key  (inv_warehouse_sk) references warehouse (w_warehouse_sk);",
    "alter table promotion add constraint p_end_date foreign key  (p_end_date_sk) references date_dim (d_date_sk);",
    "alter table promotion add constraint p_i foreign key  (p_item_sk) references item (i_item_sk);",
    "alter table promotion add constraint p_start_date foreign key  (p_start_date_sk) references date_dim (d_date_sk);",
    "alter table store add constraint s_close_date foreign key  (s_closed_date_sk) references date_dim (d_date_sk);",
    "alter table store_returns add constraint sr_a foreign key  (sr_addr_sk) references customer_address (ca_address_sk);",
    "alter table store_returns add constraint sr_cd foreign key  (sr_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table store_returns add constraint sr_c foreign key  (sr_customer_sk) references customer (c_customer_sk);",
    "alter table store_returns add constraint sr_hd foreign key  (sr_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table store_returns add constraint sr_i foreign key  (sr_item_sk) references item (i_item_sk);",
    "alter table store_returns add constraint sr_r foreign key  (sr_reason_sk) references reason (r_reason_sk);",
    "alter table store_returns add constraint sr_ret_d foreign key  (sr_returned_date_sk) references date_dim (d_date_sk);",
    "alter table store_returns add constraint sr_t foreign key  (sr_return_time_sk) references time_dim (t_time_sk);",
    "alter table store_returns add constraint sr_s foreign key  (sr_store_sk) references store (s_store_sk);",
    "alter table store_sales add constraint ss_a foreign key  (ss_addr_sk) references customer_address (ca_address_sk);",
    "alter table store_sales add constraint ss_cd foreign key  (ss_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table store_sales add constraint ss_c foreign key  (ss_customer_sk) references customer (c_customer_sk);",
    "alter table store_sales add constraint ss_hd foreign key  (ss_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table store_sales add constraint ss_i foreign key  (ss_item_sk) references item (i_item_sk);",
    "alter table store_sales add constraint ss_p foreign key  (ss_promo_sk) references promotion (p_promo_sk);",
    "alter table store_sales add constraint ss_d foreign key  (ss_sold_date_sk) references date_dim (d_date_sk);",
    "alter table store_sales add constraint ss_t foreign key  (ss_sold_time_sk) references time_dim (t_time_sk);",
    "alter table store_sales add constraint ss_s foreign key  (ss_store_sk) references store (s_store_sk);",
    "alter table web_page add constraint wp_ad foreign key  (wp_access_date_sk) references date_dim (d_date_sk);",
    "alter table web_page add constraint wp_cd foreign key  (wp_creation_date_sk) references date_dim (d_date_sk);",
    "alter table web_returns add constraint wr_i foreign key  (wr_item_sk) references item (i_item_sk);",
    "alter table web_returns add constraint wr_r foreign key  (wr_reason_sk) references reason (r_reason_sk);",
    "alter table web_returns add constraint wr_ref_a foreign key  (wr_refunded_addr_sk) references customer_address (ca_address_sk);",
    "alter table web_returns add constraint wr_ref_cd foreign key  (wr_refunded_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table web_returns add constraint wr_ref_c foreign key  (wr_refunded_customer_sk) references customer (c_customer_sk);",
    "alter table web_returns add constraint wr_ref_hd foreign key  (wr_refunded_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table web_returns add constraint wr_ret_d foreign key  (wr_returned_date_sk) references date_dim (d_date_sk);",
    "alter table web_returns add constraint wr_ret_t foreign key  (wr_returned_time_sk) references time_dim (t_time_sk);",
    "alter table web_returns add constraint wr_ret_a foreign key  (wr_returning_addr_sk) references customer_address (ca_address_sk);",
    "alter table web_returns add constraint wr_ret_cd foreign key  (wr_returning_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table web_returns add constraint wr_ret_c foreign key  (wr_returning_customer_sk) references customer (c_customer_sk);",
    "alter table web_returns add constraint wr_ret_hd foreign key  (wr_returning_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table web_returns add constraint wr_wp foreign key  (wr_web_page_sk) references web_page (wp_web_page_sk);",
    "alter table web_sales add constraint ws_b_a foreign key  (ws_bill_addr_sk) references customer_address (ca_address_sk);",
    "alter table web_sales add constraint ws_b_cd foreign key  (ws_bill_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table web_sales add constraint ws_b_c foreign key  (ws_bill_customer_sk) references customer (c_customer_sk);",
    "alter table web_sales add constraint ws_b_hd foreign key  (ws_bill_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table web_sales add constraint ws_i foreign key  (ws_item_sk) references item (i_item_sk);",
    "alter table web_sales add constraint ws_p foreign key  (ws_promo_sk) references promotion (p_promo_sk);",
    "alter table web_sales add constraint ws_s_a foreign key  (ws_ship_addr_sk) references customer_address (ca_address_sk);",
    "alter table web_sales add constraint ws_s_cd foreign key  (ws_ship_cdemo_sk) references customer_demographics (cd_demo_sk);",
    "alter table web_sales add constraint ws_s_c foreign key  (ws_ship_customer_sk) references customer (c_customer_sk);",
    "alter table web_sales add constraint ws_s_d foreign key  (ws_ship_date_sk) references date_dim (d_date_sk);",
    "alter table web_sales add constraint ws_s_hd foreign key  (ws_ship_hdemo_sk) references household_demographics (hd_demo_sk);",
    "alter table web_sales add constraint ws_sm foreign key  (ws_ship_mode_sk) references ship_mode (sm_ship_mode_sk);",
    "alter table web_sales add constraint ws_d2 foreign key  (ws_sold_date_sk) references date_dim (d_date_sk);",
    "alter table web_sales add constraint ws_t foreign key  (ws_sold_time_sk) references time_dim (t_time_sk);",
    "alter table web_sales add constraint ws_w2 foreign key  (ws_warehouse_sk) references warehouse (w_warehouse_sk);",
    "alter table web_sales add constraint ws_wp foreign key  (ws_web_page_sk) references web_page (wp_web_page_sk);",
    "alter table web_sales add constraint ws_ws foreign key  (ws_web_site_sk) references web_site (web_site_sk);",
    "alter table web_site add constraint web_d1 foreign key  (web_close_date_sk) references date_dim (d_date_sk);",
    "alter table web_site add constraint web_d2 foreign key  (web_open_date_sk) references date_dim (d_date_sk);"
]

# Define the batch size
batch_size = 1

# Track the execution time and success of each query
execution_times = []
success_flags = []
for batch_start in range(0, len(queries), batch_size):
    batch_queries = queries[batch_start:batch_start + batch_size]
    start_time = time.time()
    try:
        # Create a connection to the MySQL database
        connection = mysql.connector.connect(
            host=hostname,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        # Execute the batch of queries
        for query in batch_queries:
            cursor.execute(query)

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

        success_flags.extend([True] * len(batch_queries))
    except mysql.connector.Error as e:
        print(f"Error executing batch of queries: {batch_queries}, Error: {str(e)}")
        success_flags.extend([False] * len(batch_queries))
    finally:
        execution_times.append(time.time() - start_time)

# Output the execution times and success flags
for idx, (execution_time, success) in enumerate(zip(execution_times, success_flags)):
    print(f"Batch {idx + 1} Execution Time: {execution_time:.2f} seconds, Success: {success}")

#cell13
import mysql.connector
import time

# Example connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Define the list of queries to execute
queries = [
    "alter table web_sales add constraint ws_wp foreign key  (ws_web_page_sk) references web_page (wp_web_page_sk);",
    "alter table web_sales add constraint ws_ws foreign key  (ws_web_site_sk) references web_site (web_site_sk);",
    "alter table web_site add constraint web_d1 foreign key  (web_close_date_sk) references date_dim (d_date_sk);",
    "alter table web_site add constraint web_d2 foreign key  (web_open_date_sk) references date_dim (d_date_sk);"
]

# Define the batch size
batch_size = 1

# Track the execution time and success of each query
execution_times = []
success_flags = []
for batch_start in range(0, len(queries), batch_size):
    batch_queries = queries[batch_start:batch_start + batch_size]
    start_time = time.time()
    try:
        # Create a connection to the MySQL database
        connection = mysql.connector.connect(
            host=hostname,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        # Execute the batch of queries
        for query in batch_queries:
            cursor.execute(query)

        # Commit the transaction
        connection.commit()
        print(f"query done: {query}")
        # Close the cursor and connection
        cursor.close()
        connection.close()

        success_flags.extend([True] * len(batch_queries))
    except mysql.connector.Error as e:
        print(f"Error executing batch of queries: {batch_queries}, Error: {str(e)}")
        success_flags.extend([False] * len(batch_queries))
    finally:
        execution_times.append(time.time() - start_time)

# Output the execution times and success flags
for idx, (execution_time, success) in enumerate(zip(execution_times, success_flags)):
    print(f"Batch {idx + 1} Execution Time: {execution_time:.2f} seconds, Success: {success}")

#cell14
import mysql.connector
import time

# Define MySQL connection parameters
hostname = "mafuehles.mysql.database.azure.com"
port = 3306  # Default MySQL port
database = "ma"
username = "t_fuehles"
password = "12Compexpert1994-"

# Define the list of ALTER TABLE queries to execute
alter_queries = [
    "ALTER TABLE call_center ADD CONSTRAINT cc_d1 FOREIGN KEY (cc_closed_date_sk) REFERENCES date_dim (d_date_sk);",
    "ALTER TABLE call_center ADD CONSTRAINT cc_d2 FOREIGN KEY (cc_open_date_sk) REFERENCES date_dim (d_date_sk);",
    "ALTER TABLE catalog_page ADD CONSTRAINT cp_d1 FOREIGN KEY (cp_end_date_sk) REFERENCES date_dim (d_date_sk);",
    "ALTER TABLE catalog_page ADD CONSTRAINT cp_d2 FOREIGN KEY (cp_start_date_sk) REFERENCES date_dim (d_date_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_cc FOREIGN KEY (cr_call_center_sk) REFERENCES call_center (cc_call_center_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_cp FOREIGN KEY (cr_catalog_page_sk) REFERENCES catalog_page (cp_catalog_page_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_i FOREIGN KEY (cr_item_sk) REFERENCES item (i_item_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_r FOREIGN KEY (cr_reason_sk) REFERENCES reason (r_reason_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_a1 FOREIGN KEY (cr_refunded_addr_sk) REFERENCES customer_address (ca_address_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_cd1 FOREIGN KEY (cr_refunded_cdemo_sk) REFERENCES customer_demographics (cd_demo_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_c1 FOREIGN KEY (cr_refunded_customer_sk) REFERENCES customer (c_customer_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_hd1 FOREIGN KEY (cr_refunded_hdemo_sk) REFERENCES household_demographics (hd_demo_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_d1 FOREIGN KEY (cr_returned_date_sk) REFERENCES date_dim (d_date_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_t FOREIGN KEY (cr_returned_time_sk) REFERENCES time_dim (t_time_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_a2 FOREIGN KEY (cr_returning_addr_sk) REFERENCES customer_address (ca_address_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_cd2 FOREIGN KEY (cr_returning_cdemo_sk) REFERENCES customer_demographics (cd_demo_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_c2 FOREIGN KEY (cr_returning_customer_sk) REFERENCES customer (c_customer_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_hd2 FOREIGN KEY (cr_returning_hdemo_sk) REFERENCES household_demographics (hd_demo_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_sm FOREIGN KEY (cr_ship_mode_sk) REFERENCES ship_mode (sm_ship_mode_sk);",
    "ALTER TABLE catalog_returns ADD CONSTRAINT cr_w2 FOREIGN KEY (cr_warehouse_sk) REFERENCES warehouse (w_warehouse_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_b_a FOREIGN KEY (cs_bill_addr_sk) REFERENCES customer_address (ca_address_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_b_cd FOREIGN KEY (cs_bill_cdemo_sk) REFERENCES customer_demographics (cd_demo_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_b_c FOREIGN KEY (cs_bill_customer_sk) REFERENCES customer (c_customer_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_b_hd FOREIGN KEY (cs_bill_hdemo_sk) REFERENCES household_demographics (hd_demo_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_cc FOREIGN KEY (cs_call_center_sk) REFERENCES call_center (cc_call_center_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_cp FOREIGN KEY (cs_catalog_page_sk) REFERENCES catalog_page (cp_catalog_page_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_i FOREIGN KEY (cs_item_sk) REFERENCES item (i_item_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_p FOREIGN KEY (cs_promo_sk) REFERENCES promotion (p_promo_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_s_a FOREIGN KEY (cs_ship_addr_sk) REFERENCES customer_address (ca_address_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_s_cd FOREIGN KEY (cs_ship_cdemo_sk) REFERENCES customer_demographics (cd_demo_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_s_c FOREIGN KEY (cs_ship_customer_sk) REFERENCES customer (c_customer_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_d1 FOREIGN KEY (cs_ship_date_sk) REFERENCES date_dim (d_date_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_s_hd FOREIGN KEY (cs_ship_hdemo_sk) REFERENCES household_demographics (hd_demo_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_sm FOREIGN KEY (cs_ship_mode_sk) REFERENCES ship_mode (sm_ship_mode_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_d2 FOREIGN KEY (cs_sold_date_sk) REFERENCES date_dim (d_date_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_t FOREIGN KEY (cs_sold_time_sk) REFERENCES time_dim (t_time_sk);",
    "ALTER TABLE catalog_sales ADD CONSTRAINT cs_w FOREIGN KEY (cs_warehouse_sk) REFERENCES warehouse (w_warehouse_sk);",
    "ALTER TABLE customer ADD CONSTRAINT c_a FOREIGN KEY (c_current_addr_sk) REFERENCES customer_address (ca_address_sk);",
    "ALTER TABLE customer ADD CONSTRAINT c_cd FOREIGN KEY (c_current_cdemo_sk) REFERENCES customer_demographics (cd_demo_sk);",
    "ALTER TABLE customer ADD CONSTRAINT c_hd FOREIGN KEY (c_current_hdemo_sk) REFERENCES household_demographics (hd_demo_sk);",
    "ALTER TABLE customer ADD CONSTRAINT c_fsd FOREIGN KEY (c_first_sales_date_sk) REFERENCES date_dim (d_date_sk);",
    "ALTER TABLE customer ADD CONSTRAINT c_fsd2 FOREIGN KEY (c_first_shipto_date_sk) REFERENCES date_dim (d_date_sk);",
]

# Track the execution time and success of each ALTER TABLE query
alter_execution_times = []
alter_success_flags = []
for query in alter_queries:
    start_time = time.time()
    try:
        # Create a connection to the MySQL database
        connection = mysql.connector.connect(
            host=hostname,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = connection.cursor()
        
        # Execute the query
        cursor.execute(query)
        
        # Commit the transaction
        connection.commit()
        
        # Close the cursor and connection
        cursor.close()
        connection.close()
        
        alter_success_flags.append(True)
    except mysql.connector.Error as e:
        print(f"Error executing query: {query}, Error: {str(e)}")
        alter_success_flags.append(False)
    finally:
        alter_execution_times.append(time.time() - start_time)

# Output the execution times and success flags for ALTER TABLE queries
for idx, (execution_time, success) in enumerate(zip(alter_execution_times, alter_success_flags)):
    print(f"ALTER TABLE Query {idx + 1} Execution Time: {execution_time:.2f} seconds, Success: {success}")

#cell15
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