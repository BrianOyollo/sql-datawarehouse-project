# Data Warehouse Project

## Naming Conventions

- use snake_case
- use English for all names
- avoid reserved words

### Table Naming Conventions

**Bronze & Silver Rules**

- All names must start with the source system and table names must match their original names without renaming
- <**sourcesystem**>_<entity>
  - <sourcesystem>: Name  of the source system
  - <entity>: Exact table name
  - example: crm_customer_info

**Gold Rules**

- All names must be meaningful, business-aligned names for tables, starting with  the category prefic

- **<category>_<entitiy>**
  
  - <category>: Describes the role of the table, such as *dim* (dimension) or *fact* (fact table)
  
  - <entity>: Descriptive name of the table, aligned with the business domain e.g *customers*, *product*, *sales*
  
  - Examples:
    
    - **dim_customers** : Dimension table for customer data
    
    - **fact sales**: fact table containing sales transactions

### Gloassary of Category Patterns

| Pattern | Meaning          | Example                              |
| ------- | ---------------- | ------------------------------------ |
| *dim_*  | Dimension table  | *dim_customer*, *dim_product*        |
| *fact_* | Fact table       | *fact_sales*                         |
| *agg_*  | Aggregated table | *agg_customers*, *agg_sales_monthly* |

### Column Naming Conventions

**Surrogate Keys**

- All primary keys in dimension tables must use the suffix **_key** 

- <**table_name**>**_key**
  
  - <table_name>: refers to the name of the table or entity the key belongs to
  
  - **_key** : suffix indicating that this is a  surrogate key
  
  - Example: **customer_key** -> surrogate key in the dim_customers table

### Technical Columns

- All technical columns must start with the prefix *dwh_*, followed by a descriptive name indicating the column's purpose

- **dwh_<column_name>**
  
  - **dwh_**: prefix exclusively for system generated metadata
  
  - <column_name>: descriptive name indicating the column's purpose
  
  - Example: **dwh_load_date** --> system-generated column used to store the date when the record was loaded

### Stored Procedure

All stored procedures used for loading data must be follow the naming pattern: **load_<layer>**

- **<layer>** : Represents the layer being loaded e.g bronze, silver, gold

- Example: ****
  
  - **load_bronze**: stored procedure for laoding data into the Bronze layer