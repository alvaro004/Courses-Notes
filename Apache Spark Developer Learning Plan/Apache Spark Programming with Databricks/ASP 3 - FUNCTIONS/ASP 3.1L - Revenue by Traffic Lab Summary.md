
# Code Summary: Aggregating and Transforming Data with PySpark

## Code Snippet:
```python
from pyspark.sql.functions import col, sum, avg, round
chain_df = (df.groupBy(col("traffic_source"))
            .agg(
                 sum("revenue").alias("total_rev"),
                 avg("revenue").alias("avg_rev")
            )
            .sort(col("total_rev").desc())
            .limit(3)
            .withColumn("total_rev", round(col("total_rev"), 2))
            .withColumn("avg_rev", round(col("avg_rev"), 2))  
)

display(chain_df)
```

## Breakdown of Operations:

1. **Grouping by `traffic_source`:**
   - Groups the DataFrame by the `traffic_source` column.

2. **Aggregating Columns:**
   - **`sum("revenue").alias("total_rev")`**: Calculates the total revenue for each traffic source and renames the result as `total_rev`.
   - **`avg("revenue").alias("avg_rev")`**: Calculates the average revenue for each traffic source and renames the result as `avg_rev`.

3. **Sorting by `total_rev`:**
   - The DataFrame is sorted in descending order based on the `total_rev` column to display the highest revenue sources first.

4. **Limiting to Top 3 Results:**
   - The result is limited to the top 3 rows after sorting.

5. **Rounding the Values:**
   - **`round(col("total_rev"), 2)`**: Rounds the `total_rev` values to 2 decimal places.
   - **`round(col("avg_rev"), 2)`**: Rounds the `avg_rev` values to 2 decimal places.

## Display:
- The final DataFrame is displayed with the top 3 traffic sources, showing both the total revenue and average revenue, rounded to 2 decimal places.

## Key Functions:
- **`groupBy()`**: Groups the DataFrame by the specified column.
- **`agg()`**: Performs aggregation operations like sum and average.
- **`sort()`**: Sorts the DataFrame by a column.
- **`limit()`**: Limits the DataFrame to a specified number of rows.
- **`withColumn()`**: Adds or modifies a column in the DataFrame.

