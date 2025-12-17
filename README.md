<h1 align="center">End-to-End E-commerce ETL Pipeline using AWS Glue, Amazon S3 and Snowflake</h1> 
<h3 align="center">Ruben Encinas Moreno</h3>

---

## Project Overview
This project demonstrates the design and implementation of an **end-to-end batch ETL data pipeline** for an **e-commerce clickstream and transactions dataset**, using **AWS Glue**, **Amazon S3**, and **Snowflake**.

The objective of the project is to ingest raw CSV data, transform it into analytics-ready Parquet format, and load it into a cloud data warehouse following **dimensional modeling best practices**.

- **Tools**: AWS S3, AWS Glue (PySpark), AWS IAM, Snowflake
- **Pipeline Type**: Batch ETL  
- **Focus**: Cloud data engineering, data lake architecture, and analytics-ready modeling

---

## Project Architecture 
The following diagram illustrates the complete data flow from raw source files to analytics-ready tables in Snowflake.

<p align="center"> <img src="images/00_Diagram.png" alt="End-to-End Pipeline Diagram" width="800"> </p>

**High-level flow:**

1. Raw CSV files sourced from Kaggle
2. Data stored in Amazon S3 (Raw Zone)
3. Transformation using AWS Glue ETL jobs
4. Curated data written back to S3 in Parquet format (Processed Zone)
5. Data loaded into Snowflake for analytics and reporting

---

## Dataset Source
The dataset used in this project comes from Kaggle and represents e-commerce transactions, sessions, events, and customer interactions.

<p align="center"> <img src="images/01_kaggle.png" alt="Kaggle Dataset Page" width="700"> </p>

<p align="center"> <img src="images/02_kaggle_data.png" alt="Kaggle Dataset Tables" width="700"> </p>

---

## Steps I Followed

### 1. Amazon S3 Setup (Raw and Processed Zones)
An Amazon S3 bucket was created to act as the data lake, with separate folders for raw and processed data.

<p align="center"> <img src="images/03_s3_bucket.png" alt="S3 Bucket" width="700"> </p> 

<p align="center"> <img src="images/04_s3_raw.png" alt="S3 Raw Zone" width="700"> </p> 

<p align="center"> <img src="images/05_s3_processed.png" alt="S3 Processed Zone" width="700"> </p>

---

### 2. AWS Glue IAM Configuration
An IAM role was created to allow AWS Glue to read from the raw S3 zone and write transformed data to the processed zone.

<p align="center"> <img src="images/06_iam_glue.png" alt="IAM Role for Glue" width="700"> </p>

---

### 3. AWS Glue ETL Jobs (PySpark)
A separate AWS Glue job was created for each table.
The screenshot below shows the Glue job execution for the **customers** table as an example.

<p align="center"> <img src="images/07_glue_job.png" alt="Glue Job Execution" width="700"> </p>

After transformation, the data is stored in Parquet format in the processed zone.

<p align="center"> <img src="images/08_data_processed.png" alt="Processed Parquet Data" width="700"> </p>

---

### 4. Snowflake Warehouse and Integration Setup
A Snowflake virtual warehouse was created to support data loading and querying.

<p align="center"> <img src="images/09_create_wh.png" alt="Snowflake Warehouse" width="700"> </p>

A storage integration was then configured to securely connect Snowflake with Amazon S3.

<p align="center"> <img src="images/10_create_integration.png" alt="Snowflake Storage Integration" width="700"> </p>

An IAM role was created in AWS to allow Snowflake read access to the processed S3 data.

<p align="center"> <img src="images/11_iam_snowflake.png" alt="IAM Role for Snowflake" width="700"> </p>

---

### 5. Snowflake External Stages and Tables
External stages were created in Snowflake to reference the processed Parquet files in S3.

<p align="center"> <img src="images/12_stage_snowflake.png" alt="Snowflake External Stage" width="700"> </p>

Dimension and fact tables were then created following a star schema design.

<p align="center"> <img src="images/13_create_table.png" alt="Create Tables" width="700"> </p>

Data was loaded into Snowflake using `COPY INTO`.

<p align="center"> <img src="images/14_copy_into.png" alt="COPY INTO" width="700"> </p>

Once loaded, the data was validated using SELECT queries.

<p align="center"> <img src="images/15_snowflake_data.png" alt="Snowflake Data Preview" width="700"> </p>

---

### 6. Data Quality Checks
Basic data quality validations were performed to ensure data integrity.

**Primary key validation:**

<p align="center"> <img src="images/16_validation_pk.png" alt="PK Validation" width="700"> </p>

**Sanity check (orders vs order items):**

<p align="center"> <img src="images/17_sanity_check.png" alt="Sanity Check" width="700"> </p>

---

### 7. Analytics Views
Two analytical views were created to demonstrate business insights.

**Daily revenue view:**

<p align="center"> <img src="images/18_daily_revenue.png" alt="Daily Revenue View" width="700"> </p>

**Top products by revenue:**

<p align="center"> <img src="images/19_top_products.png" alt="Top Products View" width="700"> </p>

---

## Project Structure
```bash
aws-snowflake/
│
├── data/
│   └── raw/                         
│       ├── customers.csv
│       ├── products.csv
│       ├── orders.csv
│       ├── order_items.csv
│       ├── sessions.csv
│       ├── events.csv
│       └── reviews.csv
│
├── glue/                            
│   ├── customers/
│   │   └── glue_customers_etl.py
│   ├── products/
│   │   └── glue_products_etl.py
│   ├── orders/
│   │   └── glue_orders_etl.py
│   ├── order_items/
│   │   └── glue_order_items_etl.py
│   ├── sessions/
│   │   └── glue_sessions_etl.py
│   ├── events/
│   │   └── glue_events_etl.py
│   └── reviews/
│       └── glue_reviews_etl.py
│
├── snowflake/                       
│   ├── 00_setup/                    
│   ├── 01_integration/              
│   ├── 02_stages/                   
│   ├── 03_tables/                   
│   ├── 04_loads/                    
│   ├── 05_validation/               
│   └── 06_views/                    
│
├── images/                          
│
├── .gitignore
└── README.md
```

---

## Scripts Used

- **AWS Glue (PySpark)**  
  ETL logic for each table is implemented using AWS Glue jobs written in Python. These scripts handle data cleaning, type casting, and transformation from CSV to Parquet.

- **Snowflake (SQL)**  
  SQL scripts are used to create warehouses, storage integrations, external stages, dimension and fact tables, data quality validations, and analytics views.

---

## Learnings
- Designing **cloud-native ETL pipelines** using AWS services.
- Implementing **data lake architecture** with raw and processed zones.
- Writing **PySpark ETL jobs** with AWS Glue.
- Integrating **Snowflake with Amazon S3** using secure storage integrations.
- Applying **dimensional modeling** and validating data quality.

---

## Time Spent
~6 hours (design, implementation, debugging, and documentation).

---

## How to Reproduce
1. Clone this repository.
2. Upload raw CSV files to the S3 raw zone.
3. Run AWS Glue jobs to process the data.
4. Configure Snowflake storage integration and external stages.
5. Execute Snowflake SQL scripts to create tables, load data, and generate views.

---

## Credits
This project was completed as a **self-directed cloud data engineering portfolio project**.

Dataset source: Kaggle – E-commerce Transactions and Clickstream Data
