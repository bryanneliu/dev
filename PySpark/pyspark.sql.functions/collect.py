df.collect() returns Array of Row type.
df.collect()[0] returns the first element in an array (1st row).
df.collect[0][0] returns the value of the first row & first column.

collect() vs select():
select() is a transformation that returns a new DataFrame and holds the columns that are selected 
collect() is an action that returns the entire data set in an Array to the driver.


