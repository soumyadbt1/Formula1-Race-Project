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
    
    1) Created Mount Points in Databricks to Connect to ADLSg2 containers :
       
