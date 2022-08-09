'''
read CSV
'''
########################################################################
synonyms = spark.read.csv(
    "s3://synonym-expansion/data/na/20211201-0/synonyms-union/part-00000-e200b6e0-4d23-477c-9ee9-4b16b14981c5-c000.csv",
    header=True, sep="\t", quote=""
)
synonyms = spark.read.csv(args.input, header=True, sep="\t", quote="")
########################################################################

schema_cap = StructType([
    StructField("asin", StringType()),
    StructField("marketplace_id", IntegerType()),
    StructField("keywords", StringType()),
    StructField("cap_score", IntegerType()),
    StructField("confidence", IntegerType()),
])

cap = spark.read.csv(
    "s3://sourcing-cap-features-prod/projects/cap/region=NA/cap_final_partitioned/marketplace_id=*/date=20220107/",
    schema=schema_cap,
    sep="\t",
)
########################################################################

'''
write CSV
'''



'''
read parque
'''
query_asins = spark.read.parquet(args.input)

query_documents = spark.read.parquet(
    *[f"{args.input}{region}/" for region in ["na", "eu", "fe"]]
)

candidates = spark.read.parquet(f"{args.input}candidates/partition-{args.partition}/")
tommy_qu = spark.read.parquet(f"{args.input}tommy-qu/")

candidates_qba = spark.read.parquet(
    *[f"{args.input_candidates}partition-{i}/" for i in range(5)]
)

'''
write parque
'''
query_asins.write.mode("overwrite").option("encoding", "UTF-8").parquet(args.output)

candidates_qba.write.mode("overwrite").option("encoding", "UTF-8") \
        .parquet(f"{args.output}partition-{args.partition}/")

'''
read JSON
'''
solr = spark.read.json("s3://synonym-expansion/data/solr-indexables/na/4_4_1639716893862/sp*/*.json.gz")
cap = spark.read.json("s3://ssd-team/markzan/BT_Aug_2021/bt.json")
janus_all = spark.read.json("s3://synonym-expansion/data/janus/na/2021-11-29-11-05-05-720/update*.gz")



'''
s3 download txt file
'''
import boto3
s3_client = boto3.client("s3")

# Remove stop words
blocklist_path = "en-stopwords.txt"
s3_client.download_file(
    "synonym-expansion",
    "data/stopwords/en-stopwords.txt",
    blocklist_path
)
with open(blocklist_path, "r") as f:
    blocklist = f.read().splitlines()