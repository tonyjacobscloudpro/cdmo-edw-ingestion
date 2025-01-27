# cdmo-edw-ingestion
# Data Pipeline with Metadata-Driven Approach POC  (Python/Pyspark)

This GitHub project example is focused on designing and implementing **end-to-end data pipelines** for a Custom Development and Manufacturing Organization (CDMO) of beauty, personal care, fragrance, and specialty products. The goal is to move data from operational source systems into a newly created **Enterprise Data Warehouse (EDW)** for advanced reporting and analysis. 

## Technologies Used
- Azure ADLS
- Databricks Community Edition

## Workflow Description
This implementation demonstrates how to simulate data ingestion and processing through a medallion architecture (Landing, Bronze, Silver, Gold) using a metadata-driven approach. The process utilizes the faker library to generate realistic datasets for testing purposes.

### The main objectives of the code are to:
- Generate five sample datasets related to beauty industry products using the faker library.
- Store the datasets in a landing zone (e.g., Azure Data Lake Storage or equivalent).
- Use a metadata configuration file to dynamically process and route the datasets through the medallion architecture (Landing → Bronze → Silver → Gold).

### Step 1. Create medallion architecture in ADLS
- Create the required medallion architecture directories in ADLS.
  - Landing Zone: Receives incoming source files.     
      - Incoming: Receives incoming files. Files are removed after processing and put into archive. Data is not persisted.
      - Archive: Consists of all files received. In case we need to reprocess.  All data is persisted.
  - Bronze Layer: Raw data ingested from source systems into ADLS in Delta format.
  - Silver Layer: Cleaned and conformed Delta tables in ADLS. Incremental data updates using Delta MERGE.
  - Gold Layer: Aggregated Delta tables designed for analytics and BI tools.
- [View code to create containers.](https://github.com/tonyjacobscloudpro/cdmo-edw-ingestion/blob/main/notebooks/00-create-adls-directories.ipynb)

### Step 2. Create sample incoming datasets
- Extract raw data (e.g., CSV/JSON files) from source systems or APIs.  I implemented the faker library to create 5 datasets that I'd be able to use as an example for ingesting data through the medallion architecture based on metadata driven config file.
- The 5 dataset descriptions are below.
- [View code to create fake/mock datasets](https://github.com/tonyjacobscloudpro/cdmo-edw-ingestion/blob/main/notebooks/01-cdmo-datasets-creation.ipynb)
  
| Dataset              | Description                             | Columns                                                                                          | Frequency | File Format | File Naming Example                   |
|----------------------|-----------------------------------------|--------------------------------------------------------------------------------------------------|-----------|-------------|---------------------------------------|
| **ProductFormulations** | Contains details about makeup product formulas. | ProductID, ProductName, Category, FormulationType, PrimaryIngredients, LaunchDate               | Daily    | csv         | `productformula_20250126_081617.csv`  |
| **ManufactureBatch** | Tracks manufacturing batch details.     | BatchID, ProductID, BatchDate, Quantity, Status                                                 | Daily     | csv         | `manufacturebatch_20250126_081617.csv`|
| **CustomerFeedBack** | Collects reviews and feedback about products. | FeedbackID, ProductID, CustomerID, Rating, Comments, FeedbackDate                               |   Daily     | csv         | customerfeedback_20250126_081617.csv                                   |
| **SalesData**        | Tracks sales across regions.            | OrderID, CustomerID, ProductID, Quantity, TotalAmount, OrderDate                                | Daily       | csv         | sales_20250126_081617.csv                                 |
| **SupplierInformation** | Contains supplier data for raw materials. | SupplierID, SupplierName, Material, Cost, DeliveryDate                                          | Daily       | csv         | supplier_20250126_081617.csv                                  |

 ### Step 3. Create metadata driven config file
- A configuration file (metadata_config_<date>.csv) is created and uploaded to the config container in ADLS.This file drives the ingestion and transformation of datasets across the layers of the medallion architecture.
- [View code to create metadata config file](https://github.com/tonyjacobscloudpro/cdmo-edw-ingestion/blob/main/notebooks/02-create-metadata-driven-config-file.ipynb)

### Step 4. Landing Zone
- Running the code in step 2 will generate 5 daily files and upload them into the landing zone for processing.
- Landing Zone: Consists of 2 directories.  
  - Incoming: Receives incoming files that are to be processed. These files are moved to Archive after processing and this folder will be empty.
  - Archive: Consists of all files received and processed. In case we need to reprocess.  All data is persisted.

### Step 5. Load Bronze Layer
- The data from the landing zone is processed and ingested into a clean, schema-conformed Delta table format in bronze layer.
- The metadata_config_20250127.csv file contains the source to target mapping.
- Timestamp is appended to each record
- Filename is appened to each record
- [View code to load data into Bronze using metadata config file](https://github.com/tonyjacobscloudpro/cdmo-edw-ingestion/blob/main/notebooks/03-load-bronze-layer.ipynb)

### Step 6. Load Silver Layer
- Silver Layer (Conformance Container):
- Cleanses and standardizes data, enforcing conformance rules and schemas.
- Transformations: Applied basic Silver layer transformations (e.g., trimming, adding timestamps).
- [View code to load data into Silver using metadata config file](https://github.com/tonyjacobscloudpro/cdmo-edw-ingestion/blob/main/notebooks/04-load-silver-layer.ipynb)

## Key Deliverables
- **Pipelines**: Data workflows to extract, transform, and load data.
- **Staging and Production Tables**: Schema definitions for organizing data in the EDW.
- **Metadata Management**: A system to map and manage source-to-target data flows.
- **BI-Ready Views**: Pre-defined SQL views for dashboards and executive reports.

## Technologies Used
This project showcases how to build scalable and automated data pipelines using **Azure services**, including:
- Azure Data Factory
- Azure Blob Storage
- Azure SQL Database
- Power BI
- Azure DevOps

---

This project is designed to support decision-making through clean, reliable, and actionable data.
