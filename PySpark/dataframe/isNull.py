isNull():
df.filter(df.state.isNull()).show()
df.filter(col("state").isNull()).show()

isNotNull():
df.filter(df.state.isNotNull()).show()
df.filter(col("state").isNotNull()).show()

multiple columns:
df.state.isNull() & df.gender.isNull()

SQL is null:
spark.sql("SELECT * FROM DATA where STATE IS NULL")