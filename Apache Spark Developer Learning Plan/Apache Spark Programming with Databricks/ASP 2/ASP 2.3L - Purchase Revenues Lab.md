
# Purchase Revenues Lab - Summary

In this lab, we worked on extracting and analyzing purchase revenue data from a dataset of events. Below is a step-by-step breakdown of what we did, the functions we used, and why each step was important.

## Purpose of the Lab
The primary goal of this lab was to filter and refine a dataset to include only events with purchase revenue, and to remove unnecessary data. This helps in making the dataset more focused and easier to analyze, specifically for events where revenue is generated.

---

## Key Functions and Definitions

### 1. `withColumn()`
**Definition**: This function is used to create or update a column in a DataFrame.

**Example**:
```python
revenue_df = events_df.withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
```
In this example, we created a new column called `revenue` by extracting the value from `ecommerce.purchase_revenue_in_usd`.

### 2. `filter()`
**Definition**: This function is used to filter rows based on a condition.

**Example**:
```python
filtered_df = revenue_df.filter(col("revenue").isNotNull())
```
In this step, we filtered the DataFrame to only include rows where the `revenue` column is not null, meaning events where purchase revenue exists.

### 3. `drop()`
**Definition**: This function is used to drop columns from the DataFrame that are no longer needed.

**Example**:
```python
final_df = filtered_df.drop("event_name")
```
Since `event_name` contains only one unique value after filtering, we dropped it to streamline the DataFrame.

---

## Code Example - Combining All Steps
Instead of performing each step separately, we can chain all the operations in a single command:

```python
final_df = (events_df
  .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))  # Create new column for revenue
  .filter(col("revenue").isNotNull())  # Filter out rows with null revenue
  .drop("event_name")  # Drop the event_name column since it only contains one value
)

display(final_df)
```
This single command achieves the same result as performing each step individually, demonstrating the power of chaining operations in PySpark.

---

## Why We Did This
The purpose of this lab was to:
1. **Isolate Purchase Revenue**: We needed to focus only on events with a valid revenue to make the dataset more meaningful for further analysis.
2. **Remove Redundancies**: By dropping columns like `event_name`, which had only one unique value, we reduced unnecessary data and simplified the dataset.
3. **Prepare Data for Analysis**: These steps helped us prepare a cleaner, more concise dataset that is easier to analyze and work with for further insights.

By the end, we successfully created a refined dataset that only includes relevant purchase revenue data, making it more efficient for further analysis and exploration.

