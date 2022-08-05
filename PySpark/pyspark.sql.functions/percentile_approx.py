'''
pyspark.sql.functions.percentile_approx(col, percentage, accuracy=10000)
1. Returns the approximate percentile of the numeric column col which is the smallest value in the ordered col values (sorted from least to greatest) such that no more than percentage of col values is less than the value or equal to that value. The value of percentage must be between 0.0 and 1.0.
2. The accuracy parameter (default: 10000) is a positive numeric literal which controls approximation accuracy at the cost of memory. Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the approximation.
3. When percentage is an array, each value of the percentage array must be between 0.0 and 1.0. In this case, returns the approximate percentile array of column col at the given percentage array.
'''

data=[(1,), (10,), (3,), (5,), (2,), (4,), (6,), (7,), (9,), (8,)]
df=spark.createDataFrame(data).toDF("count")

from pyspark.sql.functions import expr
percentile = df.select(
        expr(f"percentile_approx(count, 0.7)")
    )

percentile.show()
+------------------------------------+
|percentile_approx(count, 0.7, 10000)|
+------------------------------------+
|                                   7|
+------------------------------------+

percentile.collect()
[Row(percentile_approx(count, 0.7, 10000)=7)]

percentile.collect()[0][0]
7