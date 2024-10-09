
# Spark Functions and Aggregations

## 1. Useful Spark Functions

### `ceil` 
- **Description:** Computes the ceiling of the given column (rounds up).
- **Example:** 
```python
df.select(ceil("column_name"))
```

### `cos`
- **Description:** Computes the cosine of the given value.
- **Example:**
```python
df.select(cos("column_name"))
```

### `log`
- **Description:** Computes the natural logarithm of the given value.
- **Example:**
```python
df.select(log("column_name"))
```

### `round`
- **Description:** Returns the value of the column rounded to 0 decimal places with HALF_UP round mode.
- **Example:**
```python
df.select(round("column_name"))
```

### `sqrt`
- **Description:** Computes the square root of the specified float value.
- **Example:**
```python
df.select(sqrt("column_name"))
```

## 2. Aggregation Functions

### `agg`
- **Description:** Compute aggregates by specifying a series of aggregate columns.
- **Example:**
```python
df.groupBy("column_name").agg(sum("column_name"), avg("column_name"))
```

### `avg`
- **Description:** Computes the mean value for each numeric column for each group.
- **Example:**
```python
df.groupBy("column_name").avg("column_name")
```

### `count`
- **Description:** Count the number of rows for each group.
- **Example:**
```python
df.groupBy("column_name").count()
```

### `max`
- **Description:** Computes the max value for each numeric column for each group.
- **Example:**
```python
df.groupBy("column_name").max("column_name")
```

### `mean`
- **Description:** Computes the average value for each numeric column for each group.
- **Example:**
```python
df.groupBy("column_name").mean("column_name")
```

### `min`
- **Description:** Computes the min value for each numeric column for each group.
- **Example:**
```python
df.groupBy("column_name").min("column_name")
```

### `pivot`
- **Description:** Pivots a column of the current DataFrame and performs the specified aggregation.
- **Example:**
```python
df.groupBy("column_name").pivot("another_column").sum("numeric_column")
```

### `sum`
- **Description:** Computes the sum for each numeric column for each group.
- **Example:**
```python
df.groupBy("column_name").sum("column_name")
```

### `approx_count_distinct`
- **Description:** Returns the approximate number of distinct items in a group.
- **Example:**
```python
df.groupBy("column_name").agg(approx_count_distinct("another_column"))
```

### `collect_list`
- **Description:** Returns a list of objects with duplicates.
- **Example:**
```python
df.groupBy("column_name").agg(collect_list("another_column"))
```

### `corr`
- **Description:** Returns the Pearson Correlation Coefficient for two columns.
- **Example:**
```python
df.agg(corr("column_1", "column_2"))
```

### `stddev_samp`
- **Description:** Returns the sample standard deviation of the expression in a group.
- **Example:**
```python
df.groupBy("column_name").agg(stddev_samp("numeric_column"))
```

### `sumDistinct`
- **Description:** Returns the sum of distinct values in the expression.
- **Example:**
```python
df.groupBy("column_name").agg(sumDistinct("numeric_column"))
```

### `var_pop`
- **Description:** Returns the population variance of the values in a group.
- **Example:**
```python
df.groupBy("column_name").agg(var_pop("numeric_column"))
```

### `.withColumn`
- **Description:** Adds a new column or replaces an existing column in a DataFrame by applying an expression or transformation to an existing column.
- **Example:**
```python
# Add a new column 'new_column' based on transformations of 'existing_column' 
df = df.withColumn("new_column", col("existing_column") * 2)
```
