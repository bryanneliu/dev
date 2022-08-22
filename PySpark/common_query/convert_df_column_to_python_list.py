##############################################
# Convert a dataframe column to a python list
##############################################
'''
flatMap
'''
dataframe.select('student Name').rdd.flatMap(lambda x: x).collect()

dataframe.select(['student Name',
                        'student Name',
                        'college']).rdd.flatMap(lambda x: x).collect()

'''
map
'''
dataframe.select('student Name').rdd.map(lambda x : x[0]).collect()

'''
collect()
'''
[data[0] for data in dataframe.select(‘column_name’).collect()]

'''
toPandas()
'''
list(dataframe.select(‘column_name’).toPandas()[‘column_name’])