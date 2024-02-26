from pyspark.sql import SparkSession
import os

def init_spark():
    spark = SparkSession.builder     .appName("lh-hms-cloud")     .config("spark.hadoop.fs.s3a.bucket.targetbucket.endpoint" ,"s3.us-south.cloud-object-storage.appdomain.cloud")     .config("spark.hadoop.fs.s3a.bucket.targetbucket.access.key" ,"accesskey")     .config("spark.hadoop.fs.s3a.bucket.targetbucket.secret.key" ,"secretkey")     .config("spark.hadoop.fs.s3a.bucket.sourcebucket.endpoint" ,"s3.us.cloud-object-storage.appdomain.cloud")     .config("spark.hadoop.fs.s3a.bucket.sourcebucket.access.key" ,"accesskey")     .config("spark.hadoop.fs.s3a.bucket.sourcebucket.secret.key" ,"secretkey")     .enableHiveSupport()     .getOrCreate()
    return spark


def create_database(spark):
    # Create a database in the lakehouse catalog
    spark.sql("create database if not exists lakehouse.localsparkdb LOCATION 's3a://sourcebucket/'")

def list_databases(spark):
    # list the database under lakehouse catalog
    spark.sql("show databases from lakehouse").show()

def basic_iceberg_table_operations(spark):
    # demonstration: Create a basic Iceberg table, insert some data and then query table
    spark.sql("create table if not exists lakehouse.localsparkdb.testTable(id INTEGER, name VARCHAR(10), age INTEGER, salary DECIMAL(10, 2)) using iceberg").show()
    spark.sql("insert into lakehouse.localsparkdb.testTable values(1,'Alan',23,3400.00),(2,'Ben',30,5500.00),(3,'Chen',35,6500.00)")
    spark.sql("select * from lakehouse.localsparkdb.testTable").show()

def create_table_from_parquet_data(spark):
    # load parquet data into dataframce
    df = spark.read.option("header",True).parquet("s3a://sourcebucket/yellow_tripdata_2022-01.parquet")
    # write the dataframe into an Iceberg table
    df.writeTo("lakehouse.localsparkdb.yellow_taxi_2022").create()
    # describe the table created
    spark.sql('describe table lakehouse.localsparkdb.yellow_taxi_2022').show(25)
    # query the table
    spark.sql('select * from lakehouse.localsparkdb.yellow_taxi_2022').count()

def ingest_from_csv_temp_table(spark):
    # load csv data into a dataframe
    csvDF = spark.read.option("header",True).csv("s3a://sourcebucket/zipcodes.csv")
    csvDF.createOrReplaceTempView("tempCSVTable")
    # load temporary table into an Iceberg table
    spark.sql('create or replace table lakehouse.localsparkdb.zipcodes using iceberg as select * from tempCSVTable')
    # describe the table created
    spark.sql('describe table lakehouse.localsparkdb.zipcodes').show(25)
    # query the table
    spark.sql('select * from lakehouse.localsparkdb.zipcodes').show()

def ingest_monthly_data(spark):
    df_feb = spark.read.option("header",True).parquet("s3a://sourcebucket/yellow_tripdata_2022-02.parquet")
    df_march = spark.read.option("header",True).parquet("s3a://sourcebucket/yellow_tripdata_2022-03.parquet")
    df_april = spark.read.option("header",True).parquet("s3a://sourcebucket/yellow_tripdata_2022-04.parquet")
    df_may = spark.read.option("header",True).parquet("s3a://sourcebucket/yellow_tripdata_2022-05.parquet")
    df_june = spark.read.option("header",True).parquet("s3a://sourcebucket/yellow_tripdata_2022-06.parquet")

    df_q1_q2 = df_feb.union(df_march).union(df_april).union(df_may).union(df_june)
    df_q1_q2.write.insertInto("lakehouse.localsparkdb.yellow_taxi_2022")

def perform_table_maintenance_operations(spark):
    # Query the metadata files table to list underlying data files
    spark.sql("SELECT file_path, file_size_in_bytes FROM lakehouse.localsparkdb.yellow_taxi_2022.files").show()

    # There are many smaller files compact them into files of 200MB each using the
    # `rewrite_data_files` Iceberg Spark procedure
    spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => 'localsparkdb.yellow_taxi_2022', options => map('target-file-size-bytes','209715200'))").show()

    # Again, query the metadata files table to list underlying data files; 6 files are compacted
    # to 3 files
    spark.sql("SELECT file_path, file_size_in_bytes FROM lakehouse.localsparkdb.yellow_taxi_2022.files").show()

    # List all the snapshots
    # Expire earlier snapshots. Only latest one with comacted data is required
    # Again, List all the snapshots to see only 1 left
    spark.sql("SELECT committed_at, snapshot_id, operation FROM lakehouse.localsparkdb.yellow_taxi_2022.snapshots").show()
    #retain only the latest one
    latest_snapshot_committed_at = spark.sql("SELECT committed_at, snapshot_id, operation FROM lakehouse.localsparkdb.yellow_taxi_2022.snapshots").tail(1)[0].committed_at
    print (latest_snapshot_committed_at)
    spark.sql(f"CALL lakehouse.system.expire_snapshots(table => 'localsparkdb.yellow_taxi_2022',older_than => TIMESTAMP '{latest_snapshot_committed_at}',retain_last => 1)").show()
    spark.sql("SELECT committed_at, snapshot_id, operation FROM lakehouse.localsparkdb.yellow_taxi_2022.snapshots").show()

    # Removing Orphan data files
    spark.sql(f"CALL lakehouse.system.remove_orphan_files(table => 'localsparkdb.yellow_taxi_2022')").show(truncate=False)

    # Rewriting Manifest Files
    spark.sql(f"CALL lakehouse.system.rewrite_manifests('localsparkdb.yellow_taxi_2022')").show()


def evolve_schema(spark):
    # demonstration: Schema evolution
    # Add column fare_per_mile to the table
    spark.sql('ALTER TABLE lakehouse.localsparkdb.yellow_taxi_2022 ADD COLUMN(fare_per_mile double)')
    # describe the table
    spark.sql('describe table lakehouse.localsparkdb.yellow_taxi_2022').show(25)


def clean_database(spark):
    # clean-up the demo database
    spark.sql('drop table if exists lakehouse.localsparkdb.testTable purge')
    spark.sql('drop table if exists lakehouse.localsparkdb.zipcodes purge')
    spark.sql('drop table if exists lakehouse.localsparkdb.yellow_taxi_2022 purge')
    spark.sql('drop database if exists lakehouse.localsparkdb cascade')

def main():
    try:
        spark = init_spark()
        clean_database(spark)

        create_database(spark)
        list_databases(spark)

        basic_iceberg_table_operations(spark)

        # demonstration: Ingest parquet and csv data into a wastonx.data Iceberg table
        create_table_from_parquet_data(spark)
        ingest_from_csv_temp_table(spark)

        # load data for the month of Feburary to June into the table yellow_taxi_2022 created above
        ingest_monthly_data(spark)

        # demonstration: Table maintenance
        perform_table_maintenance_operations(spark)

        # demonstration: Schema evolution
        evolve_schema(spark)
    finally:
        # clean-up the demo database
        #clean_database(spark)
        spark.stop()

if __name__ == '__main__':
  main()