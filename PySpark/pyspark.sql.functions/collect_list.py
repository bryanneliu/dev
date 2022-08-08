'''
Spark SQL collect_list() and collect_set() functions are used to create an array (ArrayType) column on DataFrame by merging rows, typically after group by or window partitions.
Spark SQL function collect_list() and collect_set() aggregates the data into a list and returns an ArrayType. 
collect_set() de-dupes the data and return unique values.
collect_list() returns the values as is without eliminating the duplicates.
'''

+----------+--------------+
|name      |booksInterested|
+----------+--------------+
|James     |Java          |
|James     |C#            |
|James     |Python        |
|Michael   |Java          |
|Michael   |PHP           |
|Michael   |PHP           |
|Robert    |Java          |
|Robert    |Java          |
|Robert    |Java          |
|Washington|null          |
+----------+--------------+

df2 = df.groupBy("name").agg(collect_list("booksIntersted").as("booksInterested"))

root
 |-- name: string (nullable = true)
 |-- booksInterested: array (nullable = true)
 |    |-- element: string (containsNull = true)

+----------+------------------+
|name      |booksInterested   |
+----------+------------------+
|James     |[Java, C#, Python]|
|Washington|[]                |
|Michael   |[Java, PHP, PHP]  |
|Robert    |[Java, Java, Java]|
+----------+------------------+

'''
collect_set() dedupe or eliminates the duplicates and results in unique for each value.
'''
df.groupBy("name").agg(collect_set("booksInterested")
    .as("booksInterestd"))
    .show(false)

+----------+------------------+
|name      |booksInterested    |
+----------+------------------+
|James     |[Java, C#, Python]|
|Washington|[]                |
|Michael   |[PHP, Java]       |
|Robert    |[Java]            |
+----------+------------------+
