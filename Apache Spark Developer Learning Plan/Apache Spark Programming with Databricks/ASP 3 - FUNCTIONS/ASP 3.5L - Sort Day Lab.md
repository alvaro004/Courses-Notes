
# Summary of Lab Work on Pandas UDFs and Plotting

## Tasks Performed
1. **Define a UDF to label the day of the week.**
2. **Apply the UDF to label and sort by the day of the week.**
3. **Plot active users by the day of the week as a bar graph.**

## Code Explanation

### Initial DataFrame Setup
We started by using the following code to create a DataFrame that represents the average number of active users by day of the week.

```python
from pyspark.sql.functions import approx_count_distinct, avg, col, date_format, to_date

df = (spark
      .read
      .format("delta")
      .load(DA.paths.events)
      .withColumn("ts", (col("event_timestamp") / 1e6).cast("timestamp"))
      .withColumn("date", to_date("ts"))
      .groupBy("date").agg(approx_count_distinct("user_id").alias("active_users"))
      .withColumn("day", date_format(col("date"), "E"))
      .groupBy("day").agg(avg(col("active_users")).alias("avg_users"))
     )

display(df)
```

### Explanation of Approximate Distinct Count of Active Users
- We used **`approx_count_distinct("user_id")`** to calculate the number of **distinct active users** per day. This function provides an efficient approximation of distinct counts, which is faster than calculating the exact number of distinct users.
- The **grouping** is performed by the `"date"` field, and the distinct count of `"user_id"` is labeled as `"active_users"`. Afterward, we further group by `"day"` to compute the average number of users for each day of the week.

### Defining the UDF for Labeling Days of the Week
We created a UDF to label the day of the week by combining a numeric value (for sorting) and the day abbreviation:

```python
def label_day_of_week(day: str) -> str:
    dow = {"Mon": "1", "Tue": "2", "Wed": "3", "Thu": "4", "Fri": "5", "Sat": "6", "Sun": "7"}
    return dow.get(day) + "-" + day

label_dow_udf = spark.udf.register("label_dow", label_day_of_week)
```
- This function maps each day abbreviation (like "Mon") to a number for sorting purposes and adds that number to the day string.

### Applying the UDF and Sorting by Day
We then applied the UDF to label the day and sorted the DataFrame by the day:
```python
final_df = (df
            .withColumn("day", label_dow_udf(col("day")))
            .sort("day"))

display(final_df)
```

### Plotting the Data as a Bar Graph
Finally, we plotted the average number of active users by day of the week as a bar graph using Matplotlib:

```python
import matplotlib.pyplot as plt

# Convert the Spark DataFrame to a Pandas DataFrame
pandas_df = final_df.toPandas()

# Plot the data as a bar graph
plt.figure(figsize=(10, 6))
plt.bar(pandas_df['day'], pandas_df['avg_users'], color='blue')
plt.title('Average Active Users by Day of the Week')
plt.xlabel('Day of the Week')
plt.ylabel('Average Number of Active Users')
plt.xticks(rotation=45)  # Rotate the day labels for better readability
plt.show()
```

### Key Points:
- We used a **UDF** to label the day of the week for sorting purposes.
- We applied the UDF to the DataFrame and then sorted the data.
- Finally, we created a **bar graph** to visualize the average active users by the day of the week.
