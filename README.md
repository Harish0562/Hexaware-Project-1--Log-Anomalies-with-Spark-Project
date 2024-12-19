# Hexaware Project-1 Log Anomalies with Spark Project

This project demonstrates how to perform log analysis using Apache Spark on Azure Databricks. The logs are stored in an Azure Blob Storage container, and the project includes steps to read, process, and write data back to Azure storage.


**Table of Contents**

    1. Overview
    
    2. Setup and Prerequisites
    
    3. Steps in the Code
    
    4. How to Run
    
    5. Output


**Project Overview**

This repository demonstrates a simple workflow for analyzing log files using Azure Databricks and Apache Spark. The data is stored in Azure Blob Storage and is accessed via Databricks File System (DBFS). The primary use case is log anomaly detection, trend analysis, and error rate calculation.

**Setup and Prerequisites**

**Prerequisites**

    1. Azure Subscription.
    
    2. An Azure Blob Storage account with a container containing the log file (e.g., Application Logs.csv).
    
    3. Access to an Azure Databricks workspace.

**Dependencies**

    1. Apache Spark
    
    2. Azure Databricks Runtime (preferably a version with Spark 3.0+).


**Configuration**

Ensure the following values are available:

    1. Storage Account Name: projectid3
    
    2. Container Name: project
    
    3. Storage Account Key: Obtain this from Azure Portal.


**Steps in the Code**

**1. Set Up Azure Storage Configurations**

Configures the Spark session to access Azure Blob Storage.

    spark.conf.set(
    f"fs.azure.account.key.<storage_account_name>.blob.core.windows.net",
    "<storage_account_key>"
    )

**2. Read the CSV File**

Reads the Application Logs.csv file stored in Azure Blob Storage into a Spark DataFrame:

    file_path = f"wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/Application Logs.csv"
    logs_df = spark.read.option("header", "true").csv(file_path)


**3. Process the Logs**

Perform transformations and analyses (e.g., trend analysis, anomaly detection, error rate calculation).


**4. Write Processed Data**

Save the processed data back to Azure Blob Storage in a processed_data/ directory.

    output_path = f"wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/processed_data/"
    logs_df.write.mode("overwrite").option("header", "true").csv(output_path)


**How to Run**

    1. Clone the repository or copy the code into your Databricks notebook.
    
    2. Replace placeholders (<storage_account_name>, <container_name>, etc.) with actual values.
    
    3. Run the cells sequentially in the Databricks notebook.
    
    4. Verify the output in the Azure Blob Storage processed_data/ folder.


**Output**

The processed logs will be saved in Azure Blob Storage at the specified output_path. Files will be saved in CSV format with the following characteristics:

    1. Includes all transformations applied in the Spark job.
    
    2. Overwrites any existing files in the target directory.
