### 1. **Apache Spark and UDFs**

In Spark, built-in functions (like `sum()`, `avg()`, `count()`, etc.) are optimized by Spark's **Catalyst Optimizer**, a powerful query optimizer that improves query execution performance by generating an efficient execution plan. However, when using **UDFs**, Spark cannot optimize them in the same way. The UDF is treated as a black box by the Catalyst optimizer, which may lead to performance issues.

#### Why UDFs are not optimized:

- **Serialization/Deserialization**: Data from the Spark DataFrame (stored in an optimized binary format) needs to be serialized into a form that Python can process. Once the UDF runs, the result is serialized back into Spark's format. This introduces overhead.
- **Interprocess Communication (IPC)**: In Python, UDFs require data to be sent between the JVM (where Spark runs) and the Python interpreter, adding communication overhead.

### 2. **Defining and Applying a UDF in Apache Spark**

Let’s start with a basic **UDF** in Apache Spark.
Example in Spark Context:
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Define a simple Python function
def multiply_by_two(x):
    return x * 2

# Convert the function into a UDF using Spark's udf function
multiply_udf = udf(multiply_by_two, IntegerType())

# Sample DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ["numbers"])

# Apply the UDF to the DataFrame
df_with_udf = df.withColumn("doubled", multiply_udf(df["numbers"]))

# Show the results
df_with_udf.show()

```
### 3. **Execution Flow in Apache Spark**

When this UDF is applied, here’s what happens behind the scenes in Spark:

- **Driver and Executors**: The Python function `multiply_by_two` is serialized and sent to each executor where the task is running.
- **Row-level Processing**: Each executor receives a partition of the DataFrame and processes the rows. For each row, the data is passed to the UDF, transformed by the Python function, and returned back to the DataFrame.

This row-by-row processing with **serialization/deserialization** steps can slow down the process compared to built-in functions, especially on large datasets.

### 4. **Registering UDFs for SQL Queries in Spark**

In Spark, you can also register your UDFs to be used within SQL queries. When working with SQL syntax, Spark treats the UDF like any other SQL function.

#### SQL UDF Example:
```python
# Register the UDF
spark.udf.register("multiply_by_two_sql", multiply_by_two, IntegerType())

# Create a temporary view of the DataFrame to run SQL queries
df.createOrReplaceTempView("numbers_table")

# Use the UDF in an SQL query
spark.sql("SELECT numbers, multiply_by_two_sql(numbers) AS doubled FROM numbers_table").show()

```
### 5. **Python Decorator for UDFs in Spark**

Using the Python decorator `@udf` allows you to create UDFs in a more concise way, which is especially useful for larger projects with a lot of UDFs.

#### Example with Decorators:
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Define a UDF using the @udf decorator
@udf(IntegerType())
def multiply_by_two(x):
    return x * 2

# Apply this UDF to the DataFrame
df_with_decorator_udf = df.withColumn("doubled", multiply_by_two(df["numbers"]))
df_with_decorator_udf.show()

```

This simplifies the UDF definition and avoids the extra step of explicitly converting the function into a UDF.

### 6. **Pandas UDFs (Vectorized UDFs) in Apache Spark**

**Pandas UDFs** were introduced in Spark 2.3 to optimize the performance of UDFs by utilizing Pandas for vectorized operations. Instead of row-by-row processing (like standard UDFs), Pandas UDFs work on **batches of rows**, making them faster and more efficient, especially on large datasets.

#### Example of Pandas UDF:
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Define a Pandas UDF using the pandas_udf decorator
@pandas_udf("int")
def multiply_by_two_pandas(s: pd.Series) -> pd.Series:
    return s * 2

# Apply the Pandas UDF to the DataFrame
df_with_pandas_udf = df.withColumn("doubled", multiply_by_two_pandas(df["numbers"]))
df_with_pandas_udf.show()

```
### **Execution of Pandas UDFs** in Spark:

- **Batch processing**: The UDF operates on a Pandas `Series` instead of a single value, meaning it can process multiple rows at once.
- **Optimized performance**: This leads to reduced serialization and deserialization overhead since batches of data are processed at once.

**Performance Advantage**: Pandas UDFs are more efficient compared to regular UDFs because they are vectorized, i.e., they apply the operation to multiple rows simultaneously instead of one row at a time.

### 7. **Performance Considerations in Spark**

- **Regular UDFs**: Slower because they require row-by-row processing and have overhead related to serialization between JVM and Python.
- **Pandas UDFs**: Much faster because they work on vectors (batches) of rows and minimize the serialization/deserialization cost.
- **Built-in Spark functions**: Always faster than both regular UDFs and Pandas UDFs because they are optimized by the Catalyst Optimizer.

### Summary of Key Concepts:

1. **UDF (User-Defined Function)**: Custom functions that allow you to extend Spark’s functionality by using Python logic, but can be slower due to serialization overhead and Catalyst Optimizer limitations.
2. **Pandas UDF**: A faster, vectorized version of UDFs that processes batches of data at once, leveraging Pandas for optimized performance.
3. **Spark’s Catalyst Optimizer**: Built-in functions benefit from optimization here, while UDFs (both regular and Pandas) do not, but Pandas UDFs perform better than regular UDFs because of batch processing.

### Pandas/Vectorized UDFs

Pandas UDFs are available in Python to improve the efficiency of UDFs. Pandas UDFs utilize Apache Arrow to speed up computation.

- [Blog post](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
- [Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow)

![Benchmark](https://databricks.com/wp-content/uploads/2017/10/image1-4.png)

The user-defined functions are executed using:

- [Apache Arrow](https://arrow.apache.org/), an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes with near-zero (de)serialization cost
- Pandas inside the function, to work with Pandas instances and APIs

![Warning](https://files.training.databricks.com/images/icon_warn_32.png) As of Spark 3.0, you should **always** define your Pandas UDF using Python type hints.