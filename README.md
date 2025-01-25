# Formula1 Race Project

## Project Overview :
This project aims to create an end-to-end data engineering solution for ingesting, processing and transforming Formula1 Race's data and providing a presentation layer for analysts to prepare reports out of the curated data. The key components of the project include:

**Data Ingestion** :
    
   - Source: Formula1 Races data is retrieved from https://ergast.com/mrd/ which includes a mix of datasets like :
     - circuits.csv
     - races.csv
     - drivers.json
     - constructors.json
     - pitstops.json
     - results.json
     - laptimes
     - qualifying
       
   - Tool: Azure Data Factory (ADF) is utilized to orchestrate the data ingestion process via activites and pipelines.
   - Destination: The ingested raw data is stored in an Azure Data Lake Gen2 raw container.
    
**Data Transformation** :
    
   - Tool: Azure Databricks is used to perform data transformations on the raw data.
   - Process: The raw data is cleaned, structured, and prepared for analysis.
   - Output: Transformed data is written back to presentation layer container also formed as databases/tables on hive metastore using managed and external storage options in Databricks.
    
**Data Analysis and Reporting**
   - Tool: Azure Databricks is employed to load the transformed data into database tables for further analysis.
   - Python & SQL Analysis: Various Python & SQL queries are used to perform analysis on cleaned data to generate insights like dominant drivers, dominant racing teams.
   - Reporting: Power BI connects to Azure Databricks to create visual reports and dashboards, providing comprehensive insights from the final presentation layer data.

**Approach**
  - The entire project uses modern approach to handle the areas where manual intervention is needed. Like creating ADLS mount points, Writing python functions to deal with capturing correct file dates, incrementally loading of source data as races happen from history to latest, making use of ADF pipeline parameters to parameterize all manual inputs and much more.
     
**Technologies Used** :
   - Azure Data Factory
   - Azure Data Lake Gen2
   - Azure Databricks
   - Power BI

This project showcases the integration of Azure services to build a scalable, efficient data pipeline from data ingestion to visualization.

### Architecture Of Project : 
![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/architecture.jpg)

#### STEPS FOLLOWED : 
1. First uploaded raw csv and json files to raw container, basically 3 folders each containing race details of history and particular dates.
   
   ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/raw_container_incremental_files.JPG)

2. Preparation for Ingestion of data :
    
    1) Created Mount Points in Databricks to Connect to ADLSg2 containers, below is the code and snapshots :
       
       https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/8.%20mount_adls_containers_for_project.ipynb

       ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/function%20to%20mount%20fs.JPG)
       ![a](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/mount%20raw%20container.JPG)
    
    2) Created variables as common configurations for mount points to be used within other notebooks by using run commands like below :
       
       ![a](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/config%20code.JPG)
       ![b](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/run%20config.JPG)
  
       Used dbutils.widgets functionality to parameterize notebooks with inputs like "p_data_source" and "p_file_date":
       
       ![c](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/widgets.JPG)

    3) Started Ingestion of data by writing schema code and reading the files from datalake.

        https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/With%20Config%20Ingestion%20Process/1.ingest_circuits_file%20(w_config).ipynb
    
```
## Creation of Processed and Presentation databases

-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dlskm/processed";

DROP DATABASE IF EXISTS f1_presentation CASCADE;

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dlskm/presentation";

```
       
```
### Created Schema for the data to be ingested.
#StuctType = Rows
#StuctField = Columns
circuits_schema = StructType(  fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name",StringType(), True),
                                     StructField("location", StringType(), True),	
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("long", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
                                     ])
```
```
## Read circuits.csv file using parameters and variables in the path and used the circuits schema defined above.
circuits_df = spark.read \
    .option("header",True) \
        .schema (circuits_schema) \
            .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
```
```
## Select only the required columns
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("long"), col("alt"))
```
```
## Rename columns (with column renamed)
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitID", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("long", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) ## added this from widget
```
```
## Added Ingestion Date
circuits_final_df = add_ingestion_date(circuits_renamed_df)
```
```
### wrote the data back to a table "circuits" in database "f1_processed" in delta format
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")
```

                                    
       
       
