'''
Explode ([ɪkˈsploʊd]爆炸) array or list and map columns to rows
'''
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

root
 |-- name: string (nullable = true)
 |-- knownLanguages: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- properties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)

+----------+--------------+--------------------+
|      name|knownLanguages|          properties|
+----------+--------------+--------------------+
|     James| [Java, Scala]|[eye -> brown, ha...|
|   Michael|[Spark, Java,]|[eye ->, hair -> ...|
|    Robert|    [CSharp, ]|[eye -> , hair ->...|
|Washington|          null|                null|
| Jefferson|        [1, 2]|                  []|
+----------+--------------+--------------------+

'''
PySpark function explode(e: Column) is used to explode or create array or map columns to rows. 
When an array is passed to this function, it creates a new default column “col1” and it contains all array elements. 
When a map is passed, it creates two new columns one for key and one for value and each element in map split into the rows.
This will ignore elements that have null or empty. from the above example, Washington and Jefferson have null or empty values in array and map, hence the following snippet out does not contain these rows.
'''

from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()

root
 |-- name: string (nullable = true)
 |-- col: string (nullable = true)

+---------+------+
|     name|   col|
+---------+------+
|    James|  Java|
|    James| Scala|
|  Michael| Spark|
|  Michael|  Java|
|  Michael|  null|
|   Robert|CSharp|
|   Robert|      |
|Jefferson|     1|
|Jefferson|     2|
+---------+------+

##############################################################

from pyspark.sql.functions import explode
df3 = df.select(df.name,explode(df.properties))
df3.printSchema()
df3.show()

root
 |-- name: string (nullable = true)
 |-- key: string (nullable = false)
 |-- value: string (nullable = true)

+-------+----+-----+
|   name| key|value|
+-------+----+-----+
|  James| eye|brown|
|  James|hair|black|
|Michael| eye| null|
|Michael|hair|brown|
| Robert| eye|     |
| Robert|hair|  red|
+-------+----+-----+