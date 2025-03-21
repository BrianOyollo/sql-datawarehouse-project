# Data Warehousing Project (PostgreSQL)

This project implements a **data warehouse** using **PostgreSQL**, following the **Medallion Architecture**. The goal is to efficiently process, store, and analyze structured data using ETL pipelines and a **star schema** model.



### **Key Features**

**1. Data Architecture** (Medallion Architectur: Bronze → Silver → Gold)  
**2. ETL Pipelines** for data ingestion, cleaning, and transformation  
**3. Data Modeling** (star schema)



## **Data Architecture**

![](/home/brianoyollo/snap/marktext/9/.config/marktext/images/2025-03-21-15-34-32-data_architecture.jpg)

This project follows the **Medallion Architecture**, which structures data into three layers:

### **Bronze Layer** (Raw Data)

- Stores **raw** ingested data **without transformations**.
- Data is loaded into **PostgreSQL tables** via **batch processing** (full load, truncate & insert).

### **Silver Layer** (Cleaned & Standardized Data)

- Data is **cleaned**, **formatted**, and **deduplicated**.
- Uses **window functions** for handling duplicates (e.g., `ROW_NUMBER() OVER(PARTITION BY...)`).
- Transformed data is stored in structured tables for easy processing.

### **Gold Layer** (Business-Ready Data)

- Uses **PostgreSQL Views** instead of physical tables.
- Provides **aggregated, analytical, and business-ready data** for reporting.
- Ensures **data freshness** by dynamically querying the Silver Layer.



## **Data Model (Star Schema - Implemented as Views)**

### **🗂 Fact Views**

| View Name         | Description                      |
| ----------------- | -------------------------------- |
| `gold.fact_sales` | Stores transactional sales data. |

### **📌 Dimension Views**

| View Name            | Description          |
| -------------------- | -------------------- |
| `gold.dim_customers` | Customer details.    |
| `gold.dim_products`  | Product information. |

![](/home/brianoyollo/snap/marktext/9/.config/marktext/images/2025-03-21-15-36-44-data_mart.jpg)





## **Running the Project**

### **1. Move Dataset to Temporary Storage**

Before running the project, move the dataset folder to `/tmp`:

```bash
mv /path/to/dataset /tmp
```

---

### **2. Open PostgreSQL in the Project Directory**

Navigate to the project directory and open PostgreSQL:

```bash
psql -U <your_username>
```

---

### **3. Create and Switch to the Database**

Create a new database and connect to it:

```sql
CREATE DATABASE <db_name>;
\c <db_name>
```

### **4. Create Schemas**

Run the script to set up the necessary schemas:

```sql
\i sql_scripts/create_schemas.sql
```

### **5. Bronze Layer**

Execute the script to create tables in the **Bronze Layer**:

```sql
\i sql_scripts/bronze/ddl_bronze.sql -- create bronze tables
\i sql_scripts/bronze/load_bronze.sql; -- create stored procedure for laoding the tables
CALL bronze.load_bronze(); -- populate the tables
```

### **6. Silver Layer**

Run the stored procedure to load raw data into the Bronze tables:

```sql
\i sql_scripts/silver/ddl_silver.sql -- create silver tables
\i sql_scripts/silver/load_silver.sql -- create stored procedure for loading the tables
CALL bronze.load_bronze(); -- execute stored procedure
```

### **7. Gold Layer**

Run the scripts to create **Gold Layer** views (fact and dimension views):

```sql
\i sql_scripts/gold/dim_customers.sql
\i sql_scripts/gold/dim_products.sql
\i sql_scripts/gold/fact_sales.sql
```

### **8. Verify Data**

Run SQL queries to check if the data is loaded correctly:

```sql
SELECT * FROM gold.dim_customers LIMIT 10;
SELECT * FROM gold.dim_products LIMIT 10;
SELECT * FROM gold.fact_sales LIMIT 10;
```

---

### **9. Connect with pgAdmin4 (Optional)**

- Open **pgAdmin4** and refresh the schemas to view your tables and data.
- Run SQL queries directly in **pgAdmin4** for validation.
