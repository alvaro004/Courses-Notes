- read csv with pandas and databricks
- using the head() function to bring just a few elements
- how to display specif series 
- use the tail() function just to bring the lastets elements from our dataframe
- Renaming columns with this example: df = df.rename(columns={"neighbourhood": "neighborhood"})
- bring elements from our dataframes based our certain criteria based on range.  We are going to find the name of the airnb which cos is between 10 to 20 usd dollars
- Boolean Masking and Filtering
- Aggregate functions, finding the mean, example: print(df["number_of_reviews"].mean()), using the describe() function which provides a report of summary statistics on a given numerical Series. We can alaso use it to find  on a DataFrame to see it applied to every numerical column. Also the round() function. GroupBy() function grouped_df = df.groupby(["neighborhood"])[["bedrooms"]].mean().head(10)
- Reset Index: Sometimes, resetting the index back to its default integer sequence is desirable. Some cases where this might be useful include:
	Eliminating duplicates in the current index
	Transforming the current index into a column so that you can include those values in columnar computations or transformations
	Making the index consistent and contiguous following an operation that altered the DataFrames shape or structure
	To reset the index, use reset_index().
-  NaN and dropping NaN with dropna()
- Impute columns, this allow us to reeplace the missing values with relevant information, we can also set default values for the NaN valus for differents columns as this: nan_df.fillna({"security_deposit": "$0.00", "notes": "Missing"}, inplace=False)
- Write to CSV
