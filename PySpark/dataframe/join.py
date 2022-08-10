'''
DataFrame.join(other, on=None, how=None)[source]
Joins with another DataFrame, using the given join expression.

* Parameters: otherDataFrame
** Right side of the join

* on: str, list or Column, optional
** a string for the join column name, 
** a list of column names, 
** a join expression (Column), 
** or a list of Columns. 
** If on is a string or a list of strings indicating the name of the join column(s), the column(s) must exist on both sides, and this performs an equi-join.

* how: str, optional
** default inner. 
** Must be one of: inner, cross, outer, full, fullouter, full_outer, left, leftouter, left_outer, right, rightouter, right_outer, semi, leftsemi, left_semi, anti, leftanti and left_anti.
'''

Join String -> Equivalent SQL Join
inner -> INNER JOIN
outer, full, fullouter, full_outer -> FULL OUTER JOIN
left, leftouter, left_outer	-> LEFT JOIN
right, rightouter, right_outer -> RIGHT JOIN
cross	
anti, leftanti, left_anti	
semi, leftsemi, left_semi

####################################################################

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

'''Emp Dataset'''
+------+--------+---------------+-----------+-----------+------+------+
|emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|
+------+--------+---------------+-----------+-----------+------+------+
|1     |Smith   |-1             |2018       |10         |M     |3000  |
|2     |Rose    |1              |2010       |20         |M     |4000  |
|3     |Williams|1              |2010       |10         |M     |1000  |
|4     |Jones   |2              |2005       |10         |F     |2000  |
|5     |Brown   |2              |2010       |40         |      |-1    |
|6     |Brown   |2              |2010       |50         |      |-1    |
+------+--------+---------------+-----------+-----------+------+------+

'''Dept Dataset'''
+---------+-------+
|dept_name|dept_id|
+---------+-------+
|Finance  |10     |
|Marketing|20     |
|Sales    |30     |
|IT       |40     |
+---------+-------+
####################################################################
'''
Inner Join: Inner join is the default join in PySpark and it’s mostly used. 
This joins two datasets on key columns, where keys don’t match the rows get dropped from both datasets (emp & dept).
'''

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)

'''
When we apply Inner join on our datasets, It drops “emp_dept_id” 50 from “emp” and “dept_id” 30 from “dept” datasets.
'''
+------+--------+---------------+-----------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+-----------+-----------+------+------+---------+-------+
|1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
|2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
|3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
|5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
+------+--------+---------------+-----------+-----------+------+------+---------+-------+

####################################################################
'''
Outer a.k.a full, fullouter join returns all rows from both datasets, 
where join expression doesn’t match it returns null on respective record columns.
'''

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer").show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full").show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter").show(truncate=False)

'''Emp Dataset'''
+------+--------+---------------+-----------+-----------+------+------+
|emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|
+------+--------+---------------+-----------+-----------+------+------+
|1     |Smith   |-1             |2018       |10         |M     |3000  |
|2     |Rose    |1              |2010       |20         |M     |4000  |
|3     |Williams|1              |2010       |10         |M     |1000  |
|4     |Jones   |2              |2005       |10         |F     |2000  |
|5     |Brown   |2              |2010       |40         |      |-1    |
|6     |Brown   |2              |2010       |50         |      |-1    |
+------+--------+---------------+-----------+-----------+------+------+

'''Dept Dataset'''
+---------+-------+
|dept_name|dept_id|
+---------+-------+
|Finance  |10     |
|Marketing|20     |
|Sales    |30     |
|IT       |40     |
+---------+-------+

'''
From our “emp” dataset’s “emp_dept_id” with value 50 doesn’t have a record on “dept” hence dept columns have null and “dept_id” 30 doesn’t have a record in “emp” hence you see null’s on emp columns.
'''
+------+--------+---------------+-----------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+-----------+-----------+------+------+---------+-------+
|2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
|5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
|1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
|3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
|6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
|null  |null    |null           |null       |null       |null  |null  |Sales    |30     |
+------+--------+---------------+-----------+-----------+------+------+---------+-------+

####################################################################
'''
Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for that record and drops records from right where match not found.
'''

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left").show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter").show(truncate=False)

'''
From our dataset, “emp_dept_id” 5o doesn’t have a record on “dept” dataset hence, this record contains null on “dept” columns (dept_name & dept_id). and “dept_id” 30 from “dept” dataset dropped from the results.
'''

+------+--------+---------------+-----------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+-----------+-----------+------+------+---------+-------+
|1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
|2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
|3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
|4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
|5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
|6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
+------+--------+---------------+-----------+-----------+------+------+---------+-------+

###############################################################################################

'''
Right a.k.a Rightouter join is opposite of left join, here it returns all rows from the right dataset regardless of math found on the left dataset, when join expression doesn’t match, it assigns null for that record and drops records from left where match not found.
'''
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right").show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter").show(truncate=False)

'''
From our example, the right dataset “dept_id” 30 doesn’t have it on the left dataset “emp” hence, this record contains null on “emp” columns. and “emp_dept_id” 50 dropped as a match not found on left.
'''

+------+--------+---------------+-----------+-----------+------+------+---------+-------+
|emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
+------+--------+---------------+-----------+-----------+------+------+---------+-------+
|4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
|3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
|1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
|2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
|null  |null    |null           |null       |null       |null  |null  |Sales    |30     |
|5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
+------+--------+---------------+-----------+-----------+------+------+---------+-------+

###############################################################################################

'''
leftsemi join is similar to inner join difference being leftsemi join returns all columns from the left dataset and ignores all columns from the right dataset. In other words, this join returns columns from the only left dataset for the records match in the right dataset on join expression, records not matched on join expression are ignored from both left and right datasets.
The same result can be achieved using select on the result of the inner join however, using this join would be efficient.
'''
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi").show(truncate=False)

'''leftsemi join'''
+------+--------+---------------+-----------+-----------+------+------+
|emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|
+------+--------+---------------+-----------+-----------+------+------+
|1     |Smith   |-1             |2018       |10         |M     |3000  |
|2     |Rose    |1              |2010       |20         |M     |4000  |
|3     |Williams|1              |2010       |10         |M     |1000  |
|4     |Jones   |2              |2005       |10         |F     |2000  |
|5     |Brown   |2              |2010       |40         |      |-1    |
+------+--------+---------------+-----------+-----------+------+------+

###############################################################################################

'''
leftanti join does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records.
'''

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti").show(truncate=False)


+------+-----+---------------+-----------+-----------+------+------+
|emp_id|name |superior_emp_id|year_joined|emp_dept_id|gender|salary|
+------+-----+---------------+-----------+-----------+------+------+
|6     |Brown|2              |2010       |50         |      |-1    |
+------+-----+---------------+-----------+-----------+------+------+

###############################################################################################

'''inner self join'''

empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)

'''
Here, we are joining emp dataset with itself to find out superior emp_id and name for all employees.
'''
+------+--------+---------------+-----------------+
|emp_id|name    |superior_emp_id|superior_emp_name|
+------+--------+---------------+-----------------+
|2     |Rose    |1              |Smith            |
|3     |Williams|1              |Smith            |
|4     |Jones   |2              |Rose             |
|5     |Brown   |2              |Rose             |
|6     |Brown   |2              |Rose             |
+------+--------+---------------+-----------------+

###############################################################################################

'''
Since PySpark SQL support native SQL syntax, we can also write join operations after creating temporary tables on DataFrames and use these tables on spark.sql().
'''
empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id").show(truncate=False)

joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id").show(truncate=False)

###############################################################################################

'''
Join on multiple DataFrames
'''
df1.join(df2,df1.id1 == df2.id2,"inner") \
   .join(df3,df1.id1 == df3.id3,"inner")

###############################################################################################
'''
crossJoin: Returns the cartesian product with another DataFrame.
'''
df.select("age", "name").collect()
[Row(age=2, name='Alice'), Row(age=5, name='Bob')]

df2.select("name", "height").collect()
[Row(name='Tom', height=80), Row(name='Bob', height=85)]

df.crossJoin(df2.select("height")).select("age", "name", "height").collect()
[Row(age=2, name='Alice', height=80), Row(age=2, name='Alice', height=85),
 Row(age=5, name='Bob', height=80), Row(age=5, name='Bob', height=85)]

age, name     name,height    height
 2, Alice      Tom, 80        80
 5, Bob        Bob, 85        85

 age, name, height
 2,   Alice, 80
 2,   Alice, 85
 5,   Bob,   80
 5,   Bob,   85