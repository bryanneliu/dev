janus_out_s3 = "s3://{bucket}/assets/janus-candidate-asins-bn-gl-2c/date={date}".format(bucket = args['bucket'], date=args['date'])


formatted_date = datetime.strptime(args['date'], "%Y%m%d").strftime("%Y-%m-%d")
SQL = '''
SELECT DISTINCT asin, title, browseladder, region, marketplaceid as marketplace_id, glproductgrouptype as gl FROM default.janus 
WHERE ds = '{date}' 
AND region IN ({regions}) 
AND marketplaceId IN ({marketplace_ids})
{limit}
'''.format(date=formatted_date, regions=args['regions'], marketplace_ids=args['marketplace_ids'], limit=limit)

concat_ws