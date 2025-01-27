# Databricks notebook source
# DBTITLE 1,pip install packages
pip install pyspark faker azure-storage-file-datalake

# COMMAND ----------

# DBTITLE 1,Import modules, libraries, classes, and functions
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from faker import Faker
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import random
import os


# COMMAND ----------

# DBTITLE 1,Create cdmo daily datasets as csv
# Initialize Spark session
spark = SparkSession.builder.appName("ComprehensiveDataSets").getOrCreate()

# Initialize Faker
fake = Faker()

# Categories and formulation types
categories = ['Foundation', 'Lipstick', 'Mascara', 'Eyeshadow', 'Blush']
formulation_types = ['Liquid', 'Powder', 'Cream', 'Gel', 'Stick']
primary_ingredients = ['Shea Butter', 'Hyaluronic Acid', 'Vitamin E', 'Collagen', 'Aloe Vera']
status_options = ['Completed', 'In Progress', 'Failed', 'Pending']

# Generate Product Formulations
product_data = []
product_ids = []  # To store ProductIDs for relationships
for _ in range(20):
    product_id = fake.uuid4()
    product_ids.append(product_id)
    product = {
        "ProductID": product_id,
        "ProductName": fake.word().capitalize() + " " + random.choice(categories),
        "Category": random.choice(categories),
        "FormulationType": random.choice(formulation_types),
        "PrimaryIngredients": ', '.join(random.sample(primary_ingredients, 2)),
        "LaunchDate": fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d")
    }
    product_data.append(product)

# Define schema for Product Formulations
product_schema = StructType([
    StructField("ProductID", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("FormulationType", StringType(), True),
    StructField("PrimaryIngredients", StringType(), True),
    StructField("LaunchDate", StringType(), True)
])

# Create Product Formulations DataFrame
product_formulations_df = spark.createDataFrame(product_data, product_schema)

# Generate Manufacturing Batch Data
batch_data = []
for _ in range(50):
    batch = {
        "BatchID": fake.uuid4(),
        "ProductID": random.choice(product_ids),
        "BatchDate": fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d"),
        "Quantity": random.randint(100, 1000),
        "Status": random.choice(status_options)
    }
    batch_data.append(batch)

# Define schema for Manufacturing Batches
batch_schema = StructType([
    StructField("BatchID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("BatchDate", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Status", StringType(), True)
])

# Create Manufacturing Batches DataFrame
manufacturing_batch_df = spark.createDataFrame(batch_data, batch_schema)

# Generate Customer Feedback Data
customer_feedback_data = []
customer_ids = [fake.uuid4() for _ in range(30)]  # Generate unique Customer IDs
for _ in range(50):
    feedback = {
        "FeedbackID": fake.uuid4(),
        "ProductID": random.choice(product_ids),
        "CustomerID": random.choice(customer_ids),
        "Rating": random.randint(1, 5),
        "Comments": fake.sentence(),
        "FeedbackDate": fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
    }
    customer_feedback_data.append(feedback)

# Define schema for Customer Feedback
customer_feedback_schema = StructType([
    StructField("FeedbackID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Rating", IntegerType(), True),
    StructField("Comments", StringType(), True),
    StructField("FeedbackDate", StringType(), True)
])

# Create Customer Feedback DataFrame
customer_feedback_df = spark.createDataFrame(customer_feedback_data, customer_feedback_schema)

# Generate Sales Data
sales_data = []
for _ in range(50):
    sale = {
        "OrderID": fake.uuid4(),
        "CustomerID": random.choice(customer_ids),
        "ProductID": random.choice(product_ids),
        "Quantity": random.randint(1, 10),
        "TotalAmount": round(random.uniform(20, 500), 2),
        "OrderDate": fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
    }
    sales_data.append(sale)

# Define schema for Sales Data
sales_schema = StructType([
    StructField("OrderID", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("TotalAmount", FloatType(), True),
    StructField("OrderDate", StringType(), True)
])

# Create Sales Data DataFrame
sales_df = spark.createDataFrame(sales_data, sales_schema)

# Generate Supplier Information Data
supplier_data = []
supplier_ids = [fake.uuid4() for _ in range(10)]  # Generate unique Supplier IDs
for _ in range(20):
    supplier = {
        "SupplierID": random.choice(supplier_ids),
        "SupplierName": fake.company(),
        "Material": random.choice(primary_ingredients),
        "Cost": round(random.uniform(10, 100), 2),
        "DeliveryDate": fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
    }
    supplier_data.append(supplier)

# Define schema for Supplier Information
supplier_schema = StructType([
    StructField("SupplierID", StringType(), True),
    StructField("SupplierName", StringType(), True),
    StructField("Material", StringType(), True),
    StructField("Cost", FloatType(), True),
    StructField("DeliveryDate", StringType(), True)
])

# Create Supplier Information DataFrame
supplier_df = spark.createDataFrame(supplier_data, supplier_schema)

# Display the DataFrames
# print("Product Formulations Dataset:")
# product_formulations_df.display()

# print("Manufacturing Batches Dataset:")
# manufacturing_batch_df.display()

# print("Customer Feedback Dataset:")
# customer_feedback_df.display()

# print("Sales Dataset:")
# sales_df.display()

# print("Supplier Information Dataset:")
# supplier_df.display()

# Save DataFrames to CSV
product_formulations_df.coalesce(1).write.csv("dbfs:/tmp/product_formulations.csv", header=True, mode="overwrite")
manufacturing_batch_df.coalesce(1).write.csv("dbfs:/tmp/manufacturing_batches.csv", header=True, mode="overwrite")
customer_feedback_df.coalesce(1).write.csv("dbfs:/tmp/customer_feedback.csv", header=True, mode="overwrite")
sales_df.coalesce(1).write.csv("dbfs:/tmp/sales_data.csv", header=True, mode="overwrite")
supplier_df.coalesce(1).write.csv("dbfs:/tmp/supplier_information.csv", header=True, mode="overwrite")

# COMMAND ----------

# DBTITLE 1,Upload csv datasets to landing zone
# Azure Storage connection details
connection_string = "XXXXXXXXXXXXX"
container_name = "00-landing"

# Function to upload a file to ADLS
def upload_to_adls(local_path, blob_path):
    try:
        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get the blob client for the target file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        # Upload the file
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded file to ADLS: {blob_path}")
    except Exception as e:
        print(f"Error uploading file to ADLS: {e}")

# Define datasets and their corresponding ADLS folders
datasets = {
    "customerfeedback": customer_feedback_df,
    "manufacturebatch": manufacturing_batch_df,
    "productformula": product_formulations_df,
    "sales": sales_df,
    "supplier": supplier_df,
}

# Get the current date and time
current_date = datetime.now().strftime("%Y%m%d")
current_time = datetime.now().strftime("%H%M%S")

# Save DataFrames to DBFS and upload to ADLS
for dataset_name, dataframe in datasets.items():
    try:
        # Define the DBFS and local paths
        dbfs_temp_dir = f"dbfs:/tmp/{dataset_name}"  # Temporary directory for Spark output
        local_temp_dir = f"/dbfs/tmp/{dataset_name}"  # Local path for accessing DBFS files
        os.makedirs(local_temp_dir, exist_ok=True)
        
        # Save the DataFrame to DBFS as a single CSV
        dataframe.coalesce(1).write.csv(dbfs_temp_dir, header=True, mode="overwrite")
        
        # Locate the part file in the DBFS directory
        files = dbutils.fs.ls(dbfs_temp_dir)
        part_file = next(f.path for f in files if f.name.startswith("part-"))
        
        # Rename the part file to a meaningful name with date and time
        local_file_path = os.path.join(local_temp_dir, f"{dataset_name}_{current_date}_{current_time}.csv")
        dbutils.fs.cp(part_file, f"file:{local_file_path}")
        
        # Define the blob path in ADLS with the date folder and timestamped file name
        adls_incoming_blob_path = f"data/incoming/{dataset_name}/{dataset_name}_{current_date}_{current_time}.csv"
        adls_archive_blob_path = f"data/archive/{dataset_name}/{dataset_name}_{current_date}_{current_time}.csv"
        
        # Upload the file to ADLS
        upload_to_adls(local_file_path, adls_incoming_blob_path)
        upload_to_adls(local_file_path, adls_archive_blob_path)
        
        # Clean up temporary directories
        dbutils.fs.rm(dbfs_temp_dir, recurse=True)
        print(f"Dataset '{dataset_name}' successfully uploaded to ADLS as {adls_blob_path}.")
    except Exception as e:
        print(f"Error processing dataset '{dataset_name}': {e}")


# COMMAND ----------

# DBTITLE 1,Validate Datasets
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import pandas as pd

# Azure Storage connection details
connection_string = "DefaultEndpointsProtocol=https;AccountName=cdmo;AccountKey=XXXXXXXXXXXX"
container_name = "00-landing"
storage_account_name = "cdmo"


# Initialize Spark session
spark = SparkSession.builder.appName("LandingZoneValidation").getOrCreate()

# Set the Spark configuration for Azure Blob Storage
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)

# Initialize DataLakeServiceClient
datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)

# Define dataset schemas for validation
schemas = {
    "customerfeedback": StructType([
        StructField("FeedbackID", StringType(), True),
        StructField("ProductID", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Rating", IntegerType(), True),
        StructField("Comments", StringType(), True),
        StructField("FeedbackDate", StringType(), True)
    ]),
    "manufacturebatch": StructType([
        StructField("BatchID", StringType(), True),
        StructField("ProductID", StringType(), True),
        StructField("BatchDate", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Status", StringType(), True)
    ]),
    "productformula": StructType([
        StructField("ProductID", StringType(), True),
        StructField("ProductName", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("FormulationType", StringType(), True),
        StructField("PrimaryIngredients", StringType(), True),
        StructField("LaunchDate", StringType(), True)
    ]),
    "sales": StructType([
        StructField("OrderID", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("ProductID", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("TotalAmount", FloatType(), True),
        StructField("OrderDate", StringType(), True)
    ]),
    "supplier": StructType([
        StructField("SupplierID", StringType(), True),
        StructField("SupplierName", StringType(), True),
        StructField("Material", StringType(), True),
        StructField("Cost", FloatType(), True),
        StructField("DeliveryDate", StringType(), True)
    ])
}

# Function to list files in a directory
def list_files_in_directory(container_name, directory_path):
    try:
        file_system_client = datalake_service_client.get_file_system_client(container_name)
        paths = file_system_client.get_paths(path=directory_path)
        files = [path.name for path in paths if not path.is_directory]
        return files
    except Exception as e:
        print(f"Error listing files in directory '{directory_path}': {e}")
        return []

# Function to validate a dataset
def validate_dataset(dataset_name, schema, files):
    validation_results = []
    for file_path in files:
        try:
            # Construct the full file path in ADLS
            full_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{file_path}"
            print(f"Validating dataset: {dataset_name}, File: {file_path}")

            # Read the file into a DataFrame
            df = spark.read.format("csv").option("header", "true").schema(schema).load(full_path)

            # Record validation results
            record_count = df.count()
            validation_results.append({
                "Dataset": dataset_name,
                "File": file_path,
                "RecordCount": record_count,
                "SchemaValid": True
            })

        except Exception as e:
            print(f"Error validating dataset '{dataset_name}': {e}")
            validation_results.append({
                "Dataset": dataset_name,
                "File": file_path,
                "RecordCount": None,
                "SchemaValid": False
            })
    return validation_results

# Main Execution
if __name__ == "__main__":
    all_validation_results = []

    for dataset_name, schema in schemas.items():
        directory_path = f"data/incoming/{dataset_name}"  # Landing zone path for each dataset
        files = list_files_in_directory(container_name, directory_path)

        if files:
            print(f"Files found for dataset '{dataset_name}': {files}")
            validation_results = validate_dataset(dataset_name, schema, files)
            all_validation_results.extend(validation_results)
        else:
            print(f"No files found for dataset '{dataset_name}' in landing zone.")

    # Convert the validation results to a DataFrame and display
    validation_results_df = pd.DataFrame(all_validation_results)
    print(validation_results_df)

