'''
string format
'''
janus_out_s3 = "s3://{bucket}/assets/janus-candidate-asins-bn-gl-2c/date={date}".format(bucket = args['bucket'], date=args['date'])

formatted_date = datetime.strptime(args['date'], "%Y%m%d").strftime("%Y-%m-%d")
SQL = '''
SELECT DISTINCT asin, title, browseladder, region, marketplaceid as marketplace_id, glproductgrouptype as gl FROM default.janus 
WHERE ds = '{date}' 
AND region IN ({regions}) 
AND marketplaceId IN ({marketplace_ids})
{limit}
'''.format(date=formatted_date, regions=args['regions'], marketplace_ids=args['marketplace_ids'], limit=limit)

################################################################################
'''
pyspark.sql.functions provides two functions concat() and concat_ws() to concatenate DataFrame multiple columns into a single column.
It can also be used to concatenate column types string, binary, and compatible array columns.
pyspark.sql.functions.concat(*cols)
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat,col
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df2=df.select(concat(df.firstname,df.middlename,df.lastname)
              .alias("FullName"),"dob","gender","salary")
df2.show(truncate=False)

'''
concat_ws() function of Pyspark concatenates multiple string columns into a single column with a given separator or delimiter.
pyspark.sql.functions.concat_ws(sep,*cols)
'''
from pyspark.sql.functions import concat_ws,col
df3=df.select(concat_ws('_',df.firstname,df.middlename,df.lastname)
              .alias("FullName"),"dob","gender","salary")
df3.show(truncate=False)

################################################################################
'''
The lstrip() method removes any leading characters (space is the default leading character to remove)
lstrip
rstrip
'''

################################################################################
'''

'''