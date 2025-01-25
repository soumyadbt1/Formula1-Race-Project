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

2. Preparation for Ingestion of data.
   
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

    4) Similarly Ingested all other files using the same structure.

      ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/ingest%20files%20for%20each%20notebook.JPG)

    5) Wrote a single notebook to ingest all other ingestion notebook at once using dbutils.notebook.run command.
       
      ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/ingest%20all%20notebooks%20at%20once.JPG)

    6) Analysis to find the dominant drivers and teams, below is the code for it.
       
       https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/analysis/0.find_dominant_drivers.sql
       https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/analysis/1.find_dominant_teams.sql
       https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/analysis/3.viz_dominant_drivers.sql
       https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/analysis/4.viz_dominant_teams.sql

4. Transformation Phase :
    
    1) Joining circuits to races - 
      
          ```
          race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
          .select(races_df.race_id,races_df.race_year,races_df.race_name, races_df.race_date, circuits_df.circuit_location)
          ```
   2) Join the results to all dataframes -
      ```
      race_results_df = results_df.join(race_circuits_df, results_df.results_race_id == race_circuits_df.race_id) \
      .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
      .join(constructors_df, results_df.constructor_id == constructors_df.constructor_Id)
      ```
      ```
      final_df = race_results_df.select("race_id","race_year","race_name","race_date","circuit_location",
      "driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date") \
      .withColumn("created_date", current_timestamp()) \
      .withColumnRenamed("result_file_date", "file_date")
      ```
   3) Merged all data using a function to write the final curated data to presentation database tables.
      ```
      merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
      merge_delta_table(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')
      ```
   4) For Driver Standing, below is the transformation code used :
      ```
      from pyspark.sql.functions import sum , when, col, count
      driver_standing_df = race_results_df \
      .groupBy("race_year","driver_name","driver_nationality") \
      .agg(sum("points").alias("total_points"),
      count(when(col("position") == 1, True)).alias("wins"))
      ```

      ```
      from pyspark.sql.window import Window
      from pyspark.sql.functions import desc, rank
      
      driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
      final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

  5) Similar Transformation steps are taken for Constructor Standings & Calculated Race Results. All the code for each are tagged below for reference.

     https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/Trans/1.%20race_results.ipynb
     https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/Trans/2.%20Driver%20Standings.ipynb
     https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/Trans/3.%20Constructor%20Standings.ipynb
     https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Code/Trans/4.%20Calculated%20Race%20Results.ipynb

  6) Merge Python Function & SQL Code used to implement incremental data loading :

     ```
     ### Python Code :
     def merge_delta_table(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
     spark.conf.set("spark.databricks.optimizer.dynamicPartitioning", "true")
     from delta.tables import DeltaTable
     if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
         deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
         deltaTable.alias("tgt").merge(
             input_df.alias("src"),
             merge_condition)\
             .whenMatchedUpdateAll()\
             .whenNotMatchedInsertAll()\
             .execute()
     else:
     input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
     ```
     ```
     ### SQL Code :
     spark.sql(f"""
        MERGE into f1_presentation.calculated_race_results tgt
        USING race_results_updated upd
        ON tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id
        WHEN MATCHED THEN
        UPDATE SET tgt.position = upd.position,
                    tgt.points = upd.points,
                    tgt.calculated_points = upd.calculated_points,
                    tgt.updated_date = current_timestamp
        WHEN NOT MATCHED THEN
        INSERT (race_year, team_name, driver_id, driver_name, position, points, calculated_points, created_date ) 
        VALUES (race_year, team_name, driver_id, driver_name, position, points, calculated_points, current_timestamp)
        """)
      ```
4. Pipeline Orchestration for the entire Ingestion, Transformation and Loading Phases :

   1) Ingestion of all files via ADF Pipeline (pl_ingest_formula1_data) :
      ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/pl_ingest_f1_data_1.JPG)
      ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/pl_ingest_f1_data_2_for_Each_loop.JPG)

   2) Transformation ADF Pipeline (pl_transform_formula1_data) :
      ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/pl_transform_formula1_data.JPG)
      ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/pl_transformation.JPG)
      
     
   3) Created execute pipeline tasks to execute both ingestion and transformation pipelines
      ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/pipeline%20run%20of%20both%20ingestion%20%26%20transformation.JPG)
   

5. Final Presentation layer data is created :
   
   ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/presentation%20layer.JPG)

7. Tables created in Hive Metastore :
   
   ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/presentation%20database%20in%20hive%20metastore.JPG)

9. Power Connection & Reporting :
    
   ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/connected%20to%20pbi.JPG)
   ![Image](https://github.com/soumyadbt1/Formula1-Race-Project/blob/main/Snapshots/formula1%20pbi%20project.JPG)

Thats all for this Project!! Thank You!!
    
                                    
       
       
