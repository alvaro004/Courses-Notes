
# Spark SQL Lab - Key Concepts, Definitions, and Examples

---

## Key Concepts:

### 1. **DataFrame**
   - A DataFrame is a distributed collection of data organized into named columns, similar to a table in a relational database or a data frame in R/Python.
   - In Spark, DataFrames are immutable, meaning transformations on DataFrames return new DataFrames.

   **Example**:
   ```python
   # Create DataFrame from an existing Spark SQL table
   df = spark.table("events")
   ```

### 2. **SparkSession**
   - The entry point to programming with Spark. It provides methods to create DataFrames, execute SQL queries, and work with datasets.
   
   **Example**:
   ```python
   # Use SparkSession to execute a SQL query
   df = spark.sql("SELECT * FROM events")
   ```

### 3. **Schema**
   - A schema defines the structure of a DataFrame, i.e., the names and data types of its columns. You can inspect the schema of a DataFrame to understand its structure.

   **Example**:
   ```python
   # Print the schema of a DataFrame
   df.printSchema()
   ```

---

## Methods and Functions:

### 1. **Creating a DataFrame from the `events` Table**

   - To create a DataFrame from an existing Spark SQL table, you can use either `table()` or `sql()` methods.

   **Example**:
   ```python
   # Method 1: Using SparkSession's table() method
   df = spark.table("events")
   
   # Method 2: Using SparkSession's sql() method
   df = spark.sql("SELECT * FROM events")
   ```

### 2. **Display the DataFrame and Inspect its Schema**

   - You can display the contents of a DataFrame using the `show()` method, and you can inspect the schema using `printSchema()` or `schema`.

   **Example**:
   ```python
   # Show first 10 rows
   df.show(10)

   # Print the schema
   df.printSchema()

   # Get the schema object (if you want to access it programmatically)
   schema = df.schema
   ```

### 3. **Applying Transformations: Filter and Sort**

   - DataFrame transformations are used to manipulate or filter data, and they are lazily evaluated (executed only when an action is called).
   
   - **Filter**: Use `where()` to filter data based on conditions.
   - **Sort**: Use `orderBy()` to sort data.

   **Example**:
   ```python
   # Filter events where device is macOS and sort by event_timestamp
   mac_os_df = df.where("device = 'macOS'").orderBy("event_timestamp")
   ```

### 4. **Counting and Selecting Rows**

   - **count()**: Counts the number of rows in the DataFrame.
   - **take()**: Returns an array of the first n rows.

   **Example**:
   ```python
   # Count the number of macOS events
   mac_os_count = mac_os_df.count()
   
   # Take the first 5 rows from the macOS DataFrame
   first_5_rows = mac_os_df.take(5)
   ```

### 5. **Creating Temporary Views**

   - A temporary view allows you to use SQL syntax to query a DataFrame as if it were a table.

   **Example**:
   ```python
   # Create or replace a temporary view for the DataFrame
   df.createOrReplaceTempView("temp_events_view")

   # Query the temporary view using SQL
   mac_os_df_sql = spark.sql("SELECT * FROM temp_events_view WHERE device = 'macOS' ORDER BY event_timestamp")
   ```

---

## Putting It All Together - Step-by-Step Example:

```python
# Step 1: Create DataFrame from events table
df = spark.table("events")

# Step 2: Display the DataFrame and inspect schema
df.show(10)
df.printSchema()

# Step 3: Filter and sort events where the device is macOS
mac_os_df = df.where("device = 'macOS'").orderBy("event_timestamp")

# Step 4: Count the number of macOS events and take the first 5 rows
mac_os_count = mac_os_df.count()
first_5_rows = mac_os_df.take(5)

# Step 5: Create a temporary view and run the same filter using SQL
df.createOrReplaceTempView("temp_events_view")
mac_os_df_sql = spark.sql("SELECT * FROM temp_events_view WHERE device = 'macOS' ORDER BY event_timestamp")

# Display the results of the SQL query
mac_os_df_sql.show(10)
```

---

## Summary:

1. **DataFrames**: You can create a DataFrame using `spark.table()` or `spark.sql()`.
2. **Transformations**: Use `select`, `where`, and `orderBy` to filter and sort DataFrames.
3. **Actions**: Use `count()` to count rows and `take()` to retrieve a specified number of rows.
4. **Schema Inspection**: Use `printSchema()` to inspect the structure of a DataFrame.
5. **SQL Integration**: You can create temporary views and run SQL queries on DataFrames.
