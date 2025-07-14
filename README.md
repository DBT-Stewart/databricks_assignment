# databricks_assignment

This repository demonstrates a complete data engineering pipeline using **Databricks Serverless Notebooks** and **Unity Catalog Volumes**.  
The project consists of two main pipelines:

- **Question 1**: Ingest CSV files using a Medallion Architecture (Bronze → Silver → Gold)
- **Question 2**: Ingest and flatten paginated REST API data into Delta format

---

## Question 1: CSV to Delta – Medallion Architecture

### Goal
Build a 3-layer Medallion architecture using structured CSVs (Employee, Department, Country).  
Data flows through:  
→ **Bronze Layer** (Raw CSV)  
→ **Silver Layer** (Cleaned Delta)  
→ **Gold Layer** (Aggregated facts)

---

### Pipeline Steps

#### Bronze Layer
- Reads raw CSVs from the volume
- Writes CSVs to:

    /Volumes/my_catalog/my_schema/my_volume/bronze/

#### 🔹 Silver Layer
- Reads bronze CSVs using a custom schema
- Converts all column names to `snake_case`
- Adds a `load_date` column
- Writes Delta table to:

    /silver/employee_info/dim_employee

#### 🔹 Gold Layer
- Joins employee with department & country
- Performs transformations to generate fact tables:
- Total salary by department
- Employee count by department and country
- Distinct department-country pairs
- Average age by department
- Adds `at_load_date` column
- Writes output to:

    /gold/employee/fact_*

---

## Question 2: API to Delta – JSON REST Data Ingestion

### API Endpoint

https://reqres.in/api/users?page={page_number}


---

### Objective

Fetch paginated JSON data from the API and store it in Delta format.

---

### Steps

1. Fetch records page-by-page until no more data is returned
2. Drop:
   - `page`, `per_page`, `total`, `total_pages`
   - Entire `support` block
3. Flatten `data[]` records
4. Add:
   - `site_address`: derived from email (static → `reqres.in`)
   - `load_date`: current date
5. Write as Delta format to:


---

### Final Schema

| Column       | Description              |
|--------------|--------------------------|
| id           | User ID                  |
| email        | Email address            |
| first_name   | First name               |
| last_name    | Last name                |
| avatar       | Profile picture URL      |
| site_address | Static domain `reqres.in`|
| load_date    | Current date             |

---

## Tech Stack

- Databricks Serverless Workspaces
- Unity Catalog Volumes
- Delta Lake Format
- PySpark (`DataFrame API`)
- REST API (`requests` library)

---

## Contact

For any enhancements or questions, feel free to raise an issue or contribute via pull request.

