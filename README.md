# cdmo-edw-ingestion
# Data Pipeline with Metadata-Driven Approach POC  (Python/Pyspark)

This GitHub project example is focused on designing and implementing **end-to-end data pipelines** for a Custom Development and Manufacturing Organization (CDMO) of beauty, personal care, fragrance, and specialty products. The goal is to move data from operational source systems into a newly created **Enterprise Data Warehouse (EDW)** for advanced reporting and analysis. 

## Technologies Used
- Azure ADLS
- Databricks Community Edition

## Workflow Description
This implementation demonstrates how to simulate data ingestion and processing through a medallion architecture (Landing, Bronze, Silver, Gold) using a metadata-driven approach. The process utilizes the faker library to generate realistic datasets for testing purposes.

### The main objectives of the code are to:
- Generate five sample datasets related beauty products using the faker library.
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

### 1. Create sample incoming datasets.
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

 
### 2. Data Transformation and Conformance
- Processes raw data to **clean, standardize, and transform** it (e.g., normalize date formats, clean invalid entries).
- Saves the conformed data in a **Staging Container** or **Staging Tables**.

### 3. Data Warehouse Loading
- Loads conformed data from staging tables into the **EDW's production tables**.
- Uses **metadata tables** to dynamically manage and automate the data loading process.

### 4. Business Intelligence Views
- Creates **operational views** for business users, enabling them to visualize insights using BI tools like **Power BI**.
- Example insights include:
  - Sales performance
  - Manufacturing trends
  - Customer feedback analysis

### 5. Version Control and Automation
- All scripts and pipelines are **version-controlled** in the repository.
- Supports **automated deployment and execution** using Azure Data Factory and Azure DevOps.

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
