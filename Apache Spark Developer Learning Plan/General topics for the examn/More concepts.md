### 1. **Spark Architecture (Tasks, Executors, Drivers, DAGs)**

- **Tasks**:
  - A **task** is the smallest unit of work in Spark. Each task is responsible for processing a data partition and executing operations like transformations.
  - Tasks are distributed across **executors** (worker nodes) in a Spark cluster. Every partition of data gets its own task.
  
- **Executors**:
  - Executors are responsible for running tasks in parallel across partitions. Each executor runs on a worker node and manages multiple **slots** (threads) for concurrent task execution.
  - Executors also store data in memory/disk and send results back to the **driver**.

- **Driver**:
  - The **driver** orchestrates the entire Spark application by scheduling tasks, creating the Directed Acyclic Graph (DAG), and managing communication between executors.
  - There is only one driver per Spark application, and it creates and manages the physical execution plan (DAG) based on transformations and actions.

- **DAG (Directed Acyclic Graph)**:
  - Every transformation in Spark is converted into stages in a DAG, where each stage is made up of tasks. The DAG scheduler determines how to execute this plan efficiently.

### 2. **Transformations and Actions (Wide, Narrow, Shuffle)**

- **Transformations**:
  - Transformations create new DataFrames from existing ones without triggering actual computation. Examples include `select()`, `filter()`, and `groupBy()`.
  
- **Wide vs. Narrow Transformations**:
  - **Narrow transformations** (e.g., `map()`, `filter()`) involve only a single partition of data, so tasks can be executed in parallel.
  - **Wide transformations** (e.g., `groupBy()`, `join()`, `repartition()`) require shuffling data across partitions. This results in slower operations because it involves data exchange between executors.

- **Actions**:
  - Actions trigger the execution of the DAG (e.g., `count()`, `collect()`, `show()`), meaning Spark will actually perform the computation of all the transformations and return a result.

- **Shuffle**:
  - A **shuffle** is triggered when Spark needs to redistribute data (e.g., during a `groupBy()`, `join()`, or `repartition()`), which causes data to be written to disk and moved between nodes. It’s one of the most expensive operations in Spark due to its high I/O overhead.

### 3. **Fault Tolerance (RDDs and DataFrame Resilience)**

- **Resilient Distributed Datasets (RDDs)**:
  - RDDs are the fundamental data structures in Spark that provide fault tolerance through lineage. If a partition of an RDD is lost, it can be recomputed from previous transformations using this lineage.

- **Fault Tolerance Mechanism**:
  - Even when Spark DataFrames are used, they are backed by RDDs, which means they are fault-tolerant by design. DataFrames are immutable, and Spark uses lineage information to recompute lost data if a node fails.

### 4. **DataFrame Operations (Joins, Aggregations, Column Operations)**

- **Joins**:
  - Joins allow you to combine data from two DataFrames based on a key. Common types include inner joins, outer joins, and left/right joins.
  - Example: 
    ```python
    df1.join(df2, df1.key == df2.key, "inner")
    ```

- **Aggregation**:
  - Aggregations like `avg()`, `sum()`, and `count()` allow you to compute metrics based on groups.
  - Handling nulls in aggregations (`asc_nulls_first`, `asc_nulls_last`) helps you control how null values are treated in sorted results.

- **Column Operations**:
  - **Renaming Columns**: Use `withColumnRenamed()` to rename columns in DataFrames.
  - **Creating Columns**: Use `withColumn()` to add or modify a column.
  - Example: 
    ```python
    df.withColumn("new_column", F.lit("Z"))
    ```

### 5. **User-Defined Functions (UDFs)**

- **UDFs**:
  - UDFs are custom functions you write in Python to apply transformations that aren't natively supported by Spark. 
  - Example:
    ```python
    capitalize_udf = udf(lambda x: x.upper(), StringType())
    df.withColumn("capitalized_name", capitalize_udf("name"))
    ```
  - Be cautious when using UDFs, as they can be slower than Spark's built-in functions due to serialization overhead.

### 6. **Sorting and Filtering**

- **Sorting**:
  - Sorting in Spark is done using `orderBy()`. You can specify how nulls are handled using `asc_nulls_first()`, `asc_nulls_last()`, `desc_nulls_first()`, and `desc_nulls_last()`.
  
- **Filtering**:
  - Use `filter()` or `where()` to filter rows based on conditions. 
  - Example:
    ```python
    df.filter(F.col("salary") >= 1000)
    ```

### 7. **Saving DataFrames (CSV, Parquet)**

- **Saving as CSV or Parquet**:
  - You can save DataFrames in various formats, like CSV or Parquet. When saving, you can specify modes like `"error"` to avoid overwriting existing data.
  - Example:
    ```python
    df.write.mode("error").csv("/path/to/csv")
    ```

### 8. **DataFrame Column Selection and Dropping**

- **Selecting Columns**:
  - You can select specific columns using the `select()` method.
  - Example:
    ```python
    df.select("employeeID", "salary")
    ```

- **Dropping Columns**:
  - You can drop columns using `drop()`.
  - Example:
    ```python
    df.drop("department")
    ```

