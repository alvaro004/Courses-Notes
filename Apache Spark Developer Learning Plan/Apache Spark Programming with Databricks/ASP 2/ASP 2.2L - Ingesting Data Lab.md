
# Apache Spark with Databricks - Ingesting Data Lab

## 1. Reading CSV with Infer Schema

### Definition:
- **Schema Inference**: Spark automatically detects the column data types based on the values in the CSV file.
  
### Example Code:
```python
# Display the first few lines of the CSV file
print(dbutils.fs.head(single_product_csv_file_path, 1000))

# Read the CSV file with inferred schema
products_df = (
    spark.read
    .option("header", "true")   # Use the first line as headers
    .option("inferSchema", "true")  # Infer column data types
    .csv(products_csv_path)     # File path to the CSV files
)

# Show the schema of the DataFrame
products_df.printSchema()
```
---
## 2. Reading CSV with a User-Defined Schema

### Definition:
- **User-Defined Schema**: Manually specifying the structure of the data (column names and types) using `StructType`.

### Example Code:
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define schema with column names and data types
user_defined_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

# Apply the schema when reading the CSV file
products_df2 = (
    spark.read
    .option("header", "true")
    .schema(user_defined_schema)
    .csv(products_csv_path)
)

# Show schema
products_df2.printSchema()
```
---
## 3. Reading CSV with DDL Formatted String

### Definition:
- **DDL (Data Definition Language) formatted string**: A string that defines the schema of the DataFrame using a SQL-like syntax.

### Example Code:
```python
ddl_schema = """
    product_id STRING, 
    product_name STRING, 
    category STRING, 
    price DOUBLE
"""

# Apply the DDL schema when reading the CSV file
products_df3 = (
    spark.read
    .option("header", "true")
    .schema(ddl_schema)
    .csv(products_csv_path)
)

# Show schema
products_df3.printSchema()
```

### Defining an schema using data definition language syntax (DDL)

```python
ddl_schema = """ product_id STRING, product_name STRING, category STRING, price DOUBLE, quantity INT """

#showing the implementation
products_df = ( spark.read 
	.option("header", "true") # Use the first row as the header 
	.schema(ddl_schema) # Apply the DDL schema 
	.csv("path/to/products.csv") # Replace with your CSV file path 
	) 
	
# Show the schema and data 
products_df.printSchema() products_df.show()
```

---
## 4. Writing DataFrame to Delta Format

### Definition:
- **Delta Format**: A storage format that provides ACID transactions and allows for scalable and reliable data pipelines.

### Example Code:
```python
# Define the output path
products_output_path = f"{DA.paths.working_dir}/delta/products"

# Write the DataFrame to Delta format
products_df.write.format("delta").mode("overwrite").save(products_output_path)
```

---
## 5. Writing DataFrame to Parquet Format

### Definition:

- **Parquet Format**: A columnar storage format optimized for reading and writing large datasets efficiently, offering better compression and performance for analytical queries.

### Example Code:

```python
# Define the output path
products_output_path = f"{DA.paths.working_dir}/parquet/products"

# Write the DataFrame to Parquet format
products_df.write.format("parquet").mode("overwrite").save(products_output_path)

```
### Explanation:

- **Columnar Format**: Parquet stores data by columns, making it efficient for queries that read specific columns from large datasets.
- **Efficient Compression**: Parquet files take less space due to compression techniques, which reduces storage costs.
- **Performance**: Reading Parquet is faster when performing analytical queries, especially in environments like Spark.
## Additional Concepts

### Schema in Apache Spark
A schema in Spark defines the structure of a DataFrame or a dataset, specifying column names and data types. It is crucial for validation, consistency, and performance optimization.

- **Infer Schema**: Allows Spark to automatically detect the data types of each column based on the values.
- **User-Defined Schema**: Gives more control, ensuring that data is interpreted in the expected format, preventing errors or inconsistencies.

### Delta Lake
In the context of Apache Spark, **Delta** refers to **Delta Lake**, which is an open-source storage layer that brings ACID (Atomicity, Consistency, Isolation, Durability) transactions to big data workloads using Apache Spark.
**Key Features of Delta Lake:**
- **ACID Transactions**: Ensures reliable data with atomic operations, preventing partial writes and inconsistencies.
- **Data Versioning**: Supports querying historical data (time travel).
- **Schema Enforcement & Evolution**: Enforces data consistency and allows schema changes over time.
- **Efficient Reads/Writes**: Optimized for large-scale data, outperforming formats like CSV or Parquet.
- **Scalable Metadata Handling**: Efficiently manages metadata for better scalability compared to other formats.

### Functions Used:
- `dbutils.fs.head`: Reads the first few bytes of a file for inspection.
- `spark.read.option()`: Configures how Spark reads a file (e.g., whether it has headers, should infer schema).
- `StructType`, `StructField`: Define the structure of a DataFrame manually.
- `.schema()`: Applies a manually defined schema or a DDL string schema to a DataFrame.
- `.write.format("delta")`: Writes a DataFrame to Delta format for efficient data management.
