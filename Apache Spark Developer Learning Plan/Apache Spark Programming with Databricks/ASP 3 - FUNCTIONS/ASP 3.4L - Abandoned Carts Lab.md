
# Apache Spark Functions Cheat Sheet

### 1. `withColumn()`

**Purpose**:  
- Used to add a new column or update an existing column in a DataFrame.

**Syntax**:
```python
DataFrame.withColumn("new_column_name", transformation_function)
```

**Example**:  
Adding a new column `item` by exploding the `cart` column:
```python
from pyspark.sql.functions import explode, col

df = df.withColumn("item", explode(col("cart")))
```

---

### 2. `explode()`

**Purpose**:  
- Converts an array or map column into multiple rows, one for each element in the array or map.

**Syntax**:
```python
explode(column_name)
```

**Example**:  
Exploding the `cart` column which contains arrays of items:
```python
df = df.withColumn("item", explode(col("cart")))
```

---

### 3. `filter()`

**Purpose**:  
- Filters rows based on a condition.

**Syntax**:
```python
DataFrame.filter(condition)
```

**Example**:  
Filtering for rows where the `converted` column is not `False`:
```python
df_filtered = df.filter(col("converted") != False)
```

---

### 4. `isNotNull()`

**Purpose**:  
- Used to filter out rows where a specific column has non-null values.

**Syntax**:
```python
DataFrame.filter(col("column_name").isNotNull())
```

**Example**:  
Filtering rows where the `email` column is not `null`:
```python
df_filtered = df.filter(col("email").isNotNull())
```

---

### 5. `join()`

**Purpose**:  
- Used to join two DataFrames on a specified condition. Different join types (inner, left, right, outer) are available.

**Syntax**:
```python
DataFrame1.join(DataFrame2, on="column_name", how="join_type")
```

**Example**:  
Performing a left join between two DataFrames on the `user_id` column:
```python
df_joined = users_df.join(events_df, on="user_id", how="left")
```

---

### 6. `groupBy()`

**Purpose**:  
- Groups the DataFrame by one or more columns to perform aggregate operations (like sum, count, etc.).

**Syntax**:
```python
DataFrame.groupBy("column_name").agg(aggregate_function)
```

**Example**:  
Grouping by `items` and counting how many times each item was abandoned:
```python
df_grouped = df.groupBy("items").count()
```

---

### 7. `agg()`

**Purpose**:  
- Used to apply aggregate functions (such as `sum()`, `count()`, `collect_set()`) on a grouped DataFrame.

**Syntax**:
```python
DataFrame.groupBy("column_name").agg(aggregate_function)
```

**Example**:  
Collecting all unique `item_id`s for each user:
```python
df_agg = df.groupBy("user_id").agg(collect_set(col("item_id")).alias("cart"))
```

---

### 8. `collect_set()`

**Purpose**:  
- Collects unique values from a column into a set. Often used to aggregate data after a `groupBy()` operation.

**Syntax**:
```python
collect_set(column_name)
```

**Example**:  
Aggregating unique `item_id`s for each user:
```python
df_agg = df.groupBy("user_id").agg(collect_set(col("item_id")).alias("cart"))
```

---

### 9. `count()`

**Purpose**:  
- Counts the number of rows or occurrences of each group when used with `groupBy()`.

**Syntax**:
```python
DataFrame.groupBy("column_name").count()
```

**Example**:  
Counting the number of abandoned items for each product:
```python
df_count = df.groupBy("items").count()
```

---

### 10. `sort()`

**Purpose**:  
- Sorts the DataFrame by a specified column.

**Syntax**:
```python
DataFrame.sort("column_name")
```

**Example**:  
Sorting the DataFrame by `items`:
```python
df_sorted = df.sort("items")
```

---

### 11. `sum()`

**Purpose**:  
- Computes the sum of values in a column. Typically used with `agg()` or `groupBy()`.

**Syntax**:
```python
sum(column_name)
```

**Example**:  
Summing the quantity of abandoned items for each product:
```python
df_sum = df.groupBy("product_id").agg(sum("quantity").alias("total_abandoned"))
```

---

### 12. `lit()`

**Purpose**:  
- Creates a column with a constant value.

**Syntax**:
```python
from pyspark.sql.functions import lit
DataFrame.withColumn("new_column", lit(constant_value))
```

**Example**:  
Adding a new column with the value `False`:
```python
df = df.withColumn("converted", lit(False))
```

---

### 13. Types of `inner joins` in Apache Spark

**Inner Join**:  
An inner join returns only the rows that have matching values in both DataFrames.

**Syntax**:
```python
df1.join(df2, on="column_name", how="inner")
```

**Example**:
```python
df_inner = df1.join(df2, on="user_id", how="inner")
```

---

### 14. The `select()` Function

**Purpose**:  
- Used to select specific columns or create new columns based on expressions.

**Syntax**:
```python
DataFrame.select("column_name", ...)
```

**Example**:
```python
df_selected = df.select("name", "age")
```

---

### 15. `.na.fill()`

**Purpose**:  
- Used to replace `null` values with a specified value in a DataFrame.

**Syntax**:
```python
DataFrame.na.fill(value)
```

**Example**:
Filling `null` values with `False` in a DataFrame:
```python
df_filled = df.na.fill(False)
```

---

### 16. `col()` Function

**Purpose**:  
- Used to refer to a column in a DataFrame when applying transformations, expressions, or filtering conditions.

**Syntax**:
```python
col("column_name")
```

**Example**:
```python
df_filtered = df.filter(col("age") > 30)
```

---

### 17. Transformation Functions

**Purpose**:  
- Transformation functions in Apache Spark are operations that transform one DataFrame into another, but they do not execute immediately. These operations are lazy and will only be executed when an action (like `show()` or `count()`) is called.

Some examples of transformation functions:
- `select()`
- `filter()`
- `groupBy()`
- `join()`

**Example**:
```python
df_transformed = df.select("name").filter(col("age") > 25)
```

---
