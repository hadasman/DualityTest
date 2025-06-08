# Assumptions
- Only CSV and JSON are supported (with local or S3 files)
- Supported operators are `last_time_period` and `eq` but it would be easy to add more (by modifying operator.py)
- The country in the query is by the source (numbers), but this can be changed.
- Even if the time column is not selected still add it to the df for partitioning

# Performance Strategy
Without an index on the source table, there would not be much difference in terms of reading the data, unless the 
predicates are on `transaction_id`, in which case we would utilize filter pushdown in `spark.read` (relevant for 
relational DBs). 

Using spark to read, write and make transformations enables parallelism.