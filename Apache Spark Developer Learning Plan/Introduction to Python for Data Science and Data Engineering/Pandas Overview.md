# Pandas Notes

## Pandas
Pandas is a Python library used to manipulate and analyze data, often employed in data science and data analysis tasks. It allows you to work with **DataFrames**, which act similarly to tables in SQL. You can perform various operations on them, such as filtering, transforming, and aggregating data.

### Example: Creating a DataFrame
```python
import pandas as pd

# Sample DataFrame
data = [["Buddy", 3, "Australian Shepherd"], 
        ["Harley", 10, "Labrador"], 
        ["Luna", 2, "Golden Retriever"], 
        ["Bailey", 8, "Chihuahua"]]

column_names = ["Name", "Age", "Breed"]
df = pd.DataFrame(data, columns=column_names)

print(df)
```

## Series
A **Series** in pandas is essentially a one-dimensional array, similar to a column in a DataFrame. Each Series has an index that labels each item. You can extract data from a DataFrame by selecting a specific column (which is a Series) and then perform operations on that Series.

### Accessing a Series:
```python
# Accessing the 'Age' column (which is a Series)
age_series = df["Age"]
print(age_series)
```

You can access specific elements in a Series using the index:
```python
# Accessing the first element in the 'Age' Series
print(age_series[0])  # Output: 3
```

### Example of Accessing an Element by Label
```python
# Accessing Buddy's age by label
buddy_age = df.loc[df['Name'] == 'Buddy', 'Age'].values[0]
print(buddy_age)  # Output: 3
```

### Example using the Loc function to extract specific information
```python
# Extract Buddy's breed and assign it to the breed variable 
breed = df.loc[df['Name'] == 'Buddy', 'Breed'].values[0] 
# Print the extracted breed 
print(breed)
```
#### The `loc[]` Function:

- The `loc[]` function is a label-based indexer in pandas. It allows you to select rows and columns based on labels (column names and row indices) rather than positional indices. It is particularly useful when you need to filter data based on specific conditions.

## dtypes
The `dtypes` attribute of a DataFrame allows you to see the data types of each column (or Series). This is helpful for understanding how pandas is interpreting the data you're working with (e.g., integers, floats, objects/strings).

### Example: Checking Data Types
```python
print(df.dtypes)
```

### Output:
```
Name     object
Age       int64
Breed    object
dtype: object
```
- `object` refers to text (strings).
- `int64` refers to integers.

## Series Operations

### Arithmetic Operations
You can perform arithmetic operations on a pandas Series, much like working with arrays.

### Example: Adding 2 years to each dog's age:
```python
df['Age'] = df['Age'] + 2
print(df['Age'])
```

### Aggregation Functions
You can also perform aggregation operations like summing or finding the mean:
```python
# Finding the average age of the dogs
mean_age = df['Age'].mean()
print(mean_age)
```

### Selecting a Subset of Columns
You can select one or more columns from a DataFrame to work with only the relevant data.

### Example: Selecting the 'Name' and 'Age' columns:
```python
subset = df[['Name', 'Age']]
print(subset)
```

## Mutability of DataFrame Objects
By default, most pandas operations **do not modify** the original DataFrame. Instead, they return a new DataFrame or Series with the modifications applied. If you want to modify the original DataFrame, you must explicitly assign the result back to the DataFrame.

### Example:
```python
# This won't change the original DataFrame
new_df = df.drop(columns=['Breed'])

# The original df still contains the 'Breed' column
print(df.columns)

# To modify the original DataFrame
df = df.drop(columns=['Breed'])
print(df.columns)  # Now 'Breed' is removed
```


## Adding a new serie in a dataframe 

### Example:
```python
# Suppose you want to want to Create a new column called "Human Age" in our "df" that takes the dog's age and multiples it by 7.

df["Human Age"] = df["Age"] * 7 
```
### Explanation:

- `df["Age"]`: This accesses the **"Age"** column in the DataFrame.
- `df["Age"] * 7`: This multiplies each value in the **"Age"** column by 7.
- `df["Human Age"]`: This creates a new column called **"Human Age"** and assigns the result of the multiplication to it.
### Summary
- Pandas is a powerful library for data manipulation.
- A **Series** is like a column in a DataFrame, and it supports many operations.
- The `dtypes` attribute shows the data types of the columns.
- Most operations return a new DataFrame by default, without modifying the original.
