probabilities.sort("score", ascending=False).show(100, False)
probabilities.sort("transition", ascending=False).show(100, False)

from pyspark.sql.functions import desc
b.orderBy(desc("col_Name"))