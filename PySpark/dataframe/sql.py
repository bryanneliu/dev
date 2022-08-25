match_set_groupby_query.createOrReplaceTempView("table1")
head_match_rate = spark.sql(f"""
SELECT 
       avg(title_match_rate) as avg_title_match_rate,
       avg(index_match_rate) as avg_index_match_rate
FROM table1
WHERE freq >= 1888
"""
)