
# importing module
import pyspark
 
# importing sparksession from
# pyspark.sql module
from pyspark.sql import SparkSession
 
# creating sparksession and giving
# an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
 
# list  of employee data
data = [["1", "sravan", "company 1"],
        ["2", "ojaswi", "company 1"],
        ["3", "rohith", "company 2"],
        ["4", "sridevi", "company 1"],
        ["1", "sravan", "company 1"],
        ["4", "sridevi", "company 1"]]
 
# specify column names
columns = ['Employee ID', 'Employee NAME', 'Company']
 
# creating a dataframe from the
# lists of data
dataframe = spark.createDataFrame(data, columns)
 
print('Actual data in dataframe')
dataframe.show()

+-----------+-------------+---------+
|Employee ID|Employee NAME|  Company|
+-----------+-------------+---------+
|          1|       sravan|company 1|
|          2|       ojaswi|company 1|
|          3|       rohith|company 2|
|          4|      sridevi|company 1|
|          1|       sravan|company 1|
|          4|      sridevi|company 1|
+-----------+-------------+---------+

'''
Distinct data means unique data. It will remove the duplicate rows in the dataframe:
dataframe.distinct()
'''
# display distinct data
dataframe.distinct().show()
+-----------+-------------+---------+
|Employee ID|Employee NAME|  Company|
+-----------+-------------+---------+
|          1|       sravan|company 1|
|          3|       rohith|company 2|
|          4|      sridevi|company 1|
|          2|       ojaswi|company 1|
+-----------+-------------+---------+

dataframe.select(['Employee ID', 'Employee NAME']).distinct().show()
+-----------+-------------+
|Employee ID|Employee NAME|
+-----------+-------------+
|          3|       rohith|
|          2|       ojaswi|
|          1|       sravan|
|          4|      sridevi|
+-----------+-------------+

'''
dataframe.dropDuplicates()
'''
dataframe.dropDuplicates().show()
+-----------+-------------+---------+
|Employee ID|Employee NAME|  Company|
+-----------+-------------+---------+
|          1|       sravan|company 1|
|          3|       rohith|company 2|
|          4|      sridevi|company 1|
|          2|       ojaswi|company 1|
+-----------+-------------+---------+

tommy_qu = tommy_qu.drop_duplicates(["session", "keywords"])
