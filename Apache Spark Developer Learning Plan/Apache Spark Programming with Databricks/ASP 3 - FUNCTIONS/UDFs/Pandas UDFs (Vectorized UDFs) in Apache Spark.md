## Pandas UDFs (Vectorized UDFs) in Apache Spark

**Pandas UDFs** (also known as **Vectorized UDFs**) in **Apache Spark** are a more efficient way to apply Python code to distributed data. These UDFs are built on top of the **Pandas library** and allow you to apply operations to entire **batches of data** (i.e., vectors) at once, rather than processing data row by row like regular UDFs.

### What Are Pandas UDFs (Vectorized UDFs)?
Pandas UDFs allow you to use the functionality of **Pandas** in a distributed Spark environment, applying the UDF logic on **batches of rows** rather than processing one row at a time. This is especially useful for performance optimization in large datasets because:
- It reduces the overhead of Python-JVM communication.
- It processes data in batches, which allows for vectorized operations (processing multiple data points at once).
  
### Key Benefits:
1. **Batch Processing**: Processes data in batches instead of row by row, which can significantly boost performance.
2. **Efficiency**: By leveraging Pandas' fast operations, Spark can avoid many of the overheads associated with regular UDFs.
3. **Integration with Spark's Distributed Framework**: Even though Pandas typically operates in memory on a single machine, **Pandas UDFs** in Spark take advantage of Spark's parallelism by operating on each partition separately across multiple worker nodes.

### Types of Pandas UDFs in Spark

There are three main types of **Pandas UDFs** (vectorized UDFs):

1. **Scalar Pandas UDF**:
    - Works on a **column** and returns a transformed column (vectorized operations on a column of data).
    - Takes in a **Pandas Series** (a column in a Pandas DataFrame) and returns another Pandas Series.
  
   Example:
    ```python
    from pyspark.sql.functions import pandas_udf
    import pandas as pd

    # Define a Scalar Pandas UDF
    @pandas_udf("int")
    def multiply_by_two(s: pd.Series) -> pd.Series:
        return s * 2

    # Apply the Pandas UDF to a Spark DataFrame
    df = spark.createDataFrame([(1,), (2,), (3,)], ["numbers"])
    df.withColumn("doubled", multiply_by_two(df["numbers"])).show()
    ```

   **What's happening**: The function `multiply_by_two` receives a **Pandas Series** and returns a Series where each value is multiplied by two. Spark internally sends **batches** of the DataFrame to the worker nodes, and these batches are transformed into Pandas Series for efficient vectorized computation.

2. **Grouped Map Pandas UDF**:
    - Operates on a **group of rows** (a Pandas DataFrame) and returns a new Pandas DataFrame.
    - Can be used when you need to apply a custom aggregation or transformation on **groups** of data (e.g., after a `groupBy()`).

   Example:
    ```python
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    import pandas as pd

    # Define a schema for the returned DataFrame
    schema = StructType([
        StructField("key", StringType()),
        StructField("count", LongType())
    ])

    # Define a Grouped Map Pandas UDF
    @pandas_udf(schema, functionType="grouped_map")
    def count_pandas_udf(pdf: pd.DataFrame) -> pd.DataFrame:
        return pdf.groupby("key").agg({"value": "count"}).reset_index()

    # Example DataFrame
    df = spark.createDataFrame([("a", 1), ("a", 2), ("b", 3)], ["key", "value"])

    # Apply the Grouped Map Pandas UDF
    df.groupby("key").apply(count_pandas_udf).show()
    ```

   **What's happening**: For each group in the `groupBy()`, Spark sends the grouped data as a Pandas DataFrame to the UDF. You can apply any **custom transformation** or aggregation on the DataFrame. In this case, we are counting the values for each key.

3. **Grouped Aggregate Pandas UDF**:
    - Operates like a regular aggregation function, taking a **Pandas Series** as input and returning a single scalar value.
    - Can be used when you need to **aggregate** values within groups (e.g., `sum`, `avg`, etc.).

   Example:
    ```python
    from pyspark.sql.functions import pandas_udf, PandasUDFType
    import pandas as pd

    # Define a Grouped Aggregate Pandas UDF
    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(v: pd.Series) -> float:
        return v.mean()

    # Example DataFrame
    df = spark.createDataFrame([(1, "a"), (2, "a"), (3, "b")], ["value", "key"])

    # Apply the Grouped Aggregate Pandas UDF
    df.groupby("key").agg(mean_udf(df["value"])).show()
    ```

   **What's happening**: The UDF calculates the mean for the values in each group using **Pandas** functionality.

### Performance Advantage of Pandas UDFs over Regular UDFs in Spark

- **Regular UDFs** in Spark process data **row by row**, which involves a lot of **serialization** and **inter-process communication** between Python and the JVM. This can significantly slow down performance when working with large datasets.
- **Pandas UDFs** process data in **batches**. Spark sends batches of rows to the Python process, where they are converted to **Pandas DataFrames/Series**. This avoids repeated serialization and allows for vectorized (batch) operations, making it much more efficient.

### Execution Flow of Pandas UDFs in Apache Spark
1. **Partitioning**: Spark splits the data into **partitions** as it normally does for parallel processing.
2. **Batch Processing**: For each partition, Spark processes data in **batches** (i.e., groups of rows) instead of individual rows.
3. **Vectorized Operations**: The data is sent to the Python environment, where it is treated as a **Pandas Series** or **Pandas DataFrame**, allowing for fast vectorized operations.
4. **Parallel Execution**: Each batch of data is processed independently on different executors (worker nodes), allowing Spark to take full advantage of its **distributed computing architecture**.

### When to Use Pandas UDFs:
- **Numerical computations**: When your transformations can be applied in batches (e.g., mathematical computations, vectorized operations on columns).
- **Custom aggregations**: When you need to perform operations like `groupBy()` with custom logic.
- **Data transformations**: When working with columns that need complex logic which isn’t available in Spark’s built-in functions.

### When NOT to Use Pandas UDFs:
- **Small datasets**: If the dataset is small and doesn’t need distributed processing, regular Pandas (outside of Spark) would be simpler and faster.
- **Operations easily expressible with built-in Spark functions**: If Spark's built-in functions (e.g., `avg()`, `sum()`, `groupBy()`, etc.) can accomplish the task, it's better to use those as they are optimized by the **Catalyst Optimizer**.

### Summary

- **Pandas UDFs (Vectorized UDFs)** allow you to leverage **Pandas’ vectorized operations** for efficient data transformations within Spark’s distributed framework.
- They operate on **batches of rows** (in the form of Pandas Series/DataFrames) rather than individual rows, which reduces overhead and improves performance.
- Pandas UDFs are faster and more efficient than regular UDFs, especially on large datasets, because they minimize the overhead associated with serialization and inter-process communication between Python and the JVM.

