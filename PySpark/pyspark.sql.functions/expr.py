'''
PySpark expr() is a SQL function to execute SQL-like expressions and 
to use an existing DataFrame column value as an expression argument 
to Pyspark built-in functions.
'''
 #####################################################################
 
#Concatenate columns using || (sql like)
data=[("James","Bond"),("Scott","Varsa")] 
df=spark.createDataFrame(data).toDF("col1","col2") 
df.withColumn("Name",expr(" col1 ||','|| col2")).show()

+-----+-----+-----------+
| col1| col2|       Name|
+-----+-----+-----------+
|James| Bond| James,Bond|
|Scott|Varsa|Scott,Varsa|
+-----+-----+-----------+

#####################################################################

from pyspark.sql.functions import expr
data = [("James","M"),("Michael","F"),("Jen","")]
columns = ["name","gender"]
df = spark.createDataFrame(data = data, schema = columns)

#Using CASE WHEN similar to SQL.
from pyspark.sql.functions import expr
df2=df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))
df2.show()
+-------+-------+
|   name| gender|
+-------+-------+
|  James|   Male|
|Michael| Female|
|    Jen|unknown|
+-------+-------+

#####################################################################

from pyspark.sql.functions import expr
data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)] 
df=spark.createDataFrame(data).toDF("date","increment") 

#Add Month value from another column
df.select(df.date,df.increment,
     expr("add_months(date,increment)")
  .alias("inc_date")).show()

+----------+---------+----------+
|      date|increment|  inc_date|
+----------+---------+----------+
|2019-01-23|        1|2019-02-23|
|2019-06-24|        2|2019-08-24|
|2019-09-20|        3|2019-12-20|
+----------+---------+----------+

#####################################################################

# Providing alias using 'as'
from pyspark.sql.functions import expr
df.select(df.date,df.increment,
     expr("""add_months(date,increment) as inc_date""")
  ).show()
# This yields same output as above

#####################################################################

# Using Cast() Function
df.select("increment",expr("cast(increment as string) as str_increment")) \
  .printSchema()

root
 |-- increment: long (nullable = true)
 |-- str_increment: string (nullable = true)

 #####################################################################

 # Arthemetic operations
df.select(df.date,df.increment,
     expr("increment + 5 as new_increment")
  ).show()

+----------+---------+-------------+
|      date|increment|new_increment|
+----------+---------+-------------+
|2019-01-23|        1|            6|
|2019-06-24|        2|            7|
|2019-09-20|        3|            8|
+----------+---------+-------------+

 #####################################################################

#Use expr()  to filter the rows
from pyspark.sql.functions import expr
data=[(100,2),(200,3000),(500,500)] 
df=spark.createDataFrame(data).toDF("col1","col2") 
df.filter(expr("col1 == col2")).show()

+----+----+
|col1|col2|
+----+----+
| 500| 500|
+----+----+

 #####################################################################