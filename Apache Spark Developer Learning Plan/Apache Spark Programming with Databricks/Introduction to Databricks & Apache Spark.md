# Delta Lake Platform
One plataform to rule them all
- Lakehouse: one place to put all your data, analytics  and AI workloads
- datebricks lakehouse plataform, why exists? 
- Unity catalog
- Delta Lake
- ![[Pasted image 20241003100036.png]]
# Delta Lake Platform
One platform to rule them all  
- **Lakehouse**: One place to put all your data, analytics, and AI workloads. The lakehouse combines the best elements of data lakes and data warehouses, allowing for a unified data architecture where you can store, manage, and analyze structured, semi-structured, and unstructured data.
- **Databricks Lakehouse Platform**: It exists to provide a unified environment for handling all types of data workloads (from ETL to machine learning) at scale. It integrates Apache Spark, Delta Lake, and a variety of other tools to support big data processing, real-time analytics, and AI on one platform.
- **Unity Catalog**: A unified governance solution for all data and AI assets on the Databricks platform. It helps manage and secure data across multiple clouds, provides fine-grained access controls, and centralizes metadata management.
- **Delta Lake**: An open-source storage layer that brings ACID transactions to data lakes, making data reliable and faster to query. Delta Lake ensures data integrity with **ACID transactions**, **time travel** to view data at different points in time, and **schema enforcement** to prevent data quality issues. It also supports **batch and streaming** workloads seamlessly.

# Apache Spark  
**Analytics gadget for big data**: Apache Spark is a distributed computing system that allows you to process large datasets across many computers. It excels at parallel processing, making it a powerful tool for data analytics, machine learning, and real-time stream processing.

# Apache Hive  
A data warehouse system built on top of Hadoop. Hive allows you to query large datasets stored in a distributed storage environment (HDFS) using an SQL-like interface. It is widely used for data warehousing, ETL, and batch processing in big data environments.

# Parameterize via Widget  
In Databricks, you can use **widgets** to parameterize notebooks. This allows you to input variables like dropdowns, text fields, or sliders, so you can dynamically pass parameters to your notebook or job without hard-coding values.

# Spark is file-based  
Spark works primarily with **files** stored in distributed file systems (e.g., HDFS, S3, Azure Data Lake). It reads and writes data in formats such as CSV, Parquet, ORC, and Delta. Spark's ability to handle large files across multiple nodes makes it an ideal engine for processing big data workloads.

# Databricks File System  
The **Databricks File System (DBFS)** is an abstraction layer on top of scalable object storage. It allows you to store and access files (including large datasets) within the Databricks environment. DBFS enables the use of familiar file operations (e.g., reading, writing, moving files) in Databricks, while seamlessly handling storage in the cloud (e.g., AWS S3 or Azure Data Lake Storage).

# Delta Lake  
When you see **".delta"** after the dataset name, it means that the data is stored in a special format called **Delta Lake**. Here's what makes it special:  

1. **Data is safer and more reliable**: Delta Lake ensures that your data is always correct and consistent. It prevents errors that might happen if multiple people or systems are working with the same data simultaneously by enabling ACID transactions.  
2. **Faster performance**: It organizes the data better, using techniques like **data compaction** and **caching**, so when you run queries, they execute faster.  
3. **You can track changes to the data**: Delta Lake provides **time travel** capabilities, meaning you can go back and see how the data looked at any point in time. This is useful for auditing and troubleshooting.  
4. **Support for both batch and streaming data**: You can seamlessly process both real-time data streams and historical batch data with Delta Lake, making it versatile for different types of data workloads.

In short, using **Delta** format makes working with large datasets in Databricks more efficient and reliable.
