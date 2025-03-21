# Data Dictionary

This catalog provides metadata, descriptions, and data types to ensure clarity and consistency.

---

## **📘 Data Catalog for Gold Layer Tables**

### 1. `gold.dim_customers`

📌 **Description:** Contains information about customers, including demographic details.

| **Column Name**   | **Data Type**  | **Description**                              |
| ----------------- | -------------- | -------------------------------------------- |
| `customer_key`    | `BIGINT` (PK)  | Unique surrogate key for the customer.       |
| `customer_id`     | `VARCHAR(50)`  | Original customer ID from source systems.    |
| `customer_number` | `VARCHAR(50)`  | Unique identifier assigned to customers.     |
| `first_name`      | `VARCHAR(100)` | Customer's first name.                       |
| `last_name`       | `VARCHAR(100)` | Customer's last name.                        |
| `marital_status`  | `VARCHAR(10)`  | Marital status (`Single`, `Married`, `N/A`). |
| `gender`          | `VARCHAR(10)`  | Customer gender (`Male`, `Female`, `N/A`).   |
| `birthdate`       | `DATE`         | Date of birth of the customer.               |
| `country`         | `VARCHAR(100)` | Country of residence.                        |

---

### **2. `gold.fact_sales`**

📌 **Description:** Stores transactional sales data linking customers and products.

| **Column Name** | **Data Type**   | **Description**                         |
| --------------- | --------------- | --------------------------------------- |
| `order_number`  | `BIGINT` (PK)   | Unique identifier for each order.       |
| `product_key`   | `BIGINT` (FK)   | Foreign key linking to `dim_products`.  |
| `customer_key`  | `BIGINT` (FK)   | Foreign key linking to `dim_customers`. |
| `order_date`    | `DATE`          | Date when the order was placed.         |
| `ship_date`     | `DATE`          | Date when the order was shipped.        |
| `due_date`      | `DATE`          | Expected delivery date.                 |
| `quantity`      | `INT`           | Number of units sold in the order.      |
| `price`         | `DECIMAL(10,2)` | Price per unit of the product.          |
| `sales`         | `DECIMAL(12,2)` | Total revenue (`quantity * price`).     |

---

### **3. `gold.dim_products`**

📌 **Description:** Stores product details, including categories and pricing.

| **Column Name**  | **Data Type**   | **Description**                             |
| ---------------- | --------------- | ------------------------------------------- |
| `product_key`    | `BIGINT` (PK)   | Unique surrogate key for each product.      |
| `product_id`     | `VARCHAR(50)`   | Original product ID from source systems.    |
| `product_number` | `VARCHAR(50)`   | Product's unique identifier.                |
| `product_name`   | `VARCHAR(255)`  | Name of the product.                        |
| `category_id`    | `VARCHAR(50)`   | Identifier for the product category.        |
| `category`       | `VARCHAR(100)`  | Product category (e.g., `Electronics`).     |
| `subcategory`    | `VARCHAR(100)`  | Product subcategory (e.g., `Laptops`).      |
| `maintenance`    | `VARCHAR(50)`   | Maintenance type (`Regular`, `None`, etc.). |
| `cost`           | `DECIMAL(12,2)` | Manufacturing or acquisition cost.          |
| `product_line`   | `VARCHAR(100)`  | High-level classification of the product.   |
| `start_date`     | `DATE`          | Date when the product became available.     |

---

### **🔗 Relationships Between Tables**

- **`fact_sales.customer_key`** → 🔗 **`dim_customers.customer_key`**
- **`fact_sales.product_key`** → 🔗 **`dim_products.product_key`**
