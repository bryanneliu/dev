'''
PySpark union() and unionAll() transformations are used to merge two or more DataFrame’s of the same schema or structure.
If schemas are not the same it returns an error.
unionAll() is deprecated since Spark “2.0.0” version and replaced with union().
'''
unionDF = df.union(df2)
disDF = df.union(df2).distinct()

americans:
+----------+---+
|first_name|age|
+----------+---+
|       bob| 42|
|      lisa| 59|
+----------+---+

colobians:
+----------+---+
|first_name|age|
+----------+---+
|     maria| 20|
|    camilo| 31|
+----------+---+

brasilians:
+---+----------+
|age|first_name|
+---+----------+
| 33|     tiago|
| 36|     lilly|
+---+----------+

americans.union(colobians) is correct
americans.union(brasilians) is wrong
americans.unionByName(brasilians) is correct