- I used the magic command ```%fs``` to see what files do i will working on in the ``DBFS``
	- `%fs` is a shorthand way of interacting with the DBFS.
	- It’s simple and convenient when you want to quickly execute commands in a notebook without embedding them in Python code.
	- The `%` magic commands are specific to Databricks notebooks.

- **`dbutils.fs.ls()` (Python API)** was used just to use both approaches, the do the same thing
	- This is the **Python API** approach to interacting with DBFS. It allows for more flexibility, as it can be used as part of a larger Python script.
	- You can manipulate the results as a **Python object** (in this case, the variable `files`) and perform additional Python operations on it, such as filtering or looping through the data.
	- It integrates well with Python code in the notebook, allowing you to use functions like `display()` to render the data in a cleaner way.
	
In summary:
- `%fs ls` is quicker and useful for one-off commands in notebooks.
- `dbutils.fs.ls()` is better when you need to incorporate the results into a more complex Python workflow.
- 
