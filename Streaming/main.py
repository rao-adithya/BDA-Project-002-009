from pyspark import SparkContext
from pyspark.streaming import StreamingContext,DStream
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
import json
from pyspark.sql.functions import desc,col
from collections import OrderedDict
from pyspark.sql.functions import isnull, when, count
from pyspark.sql.functions import hour, month, year
import matplotlib.pyplot as plt
import seaborn as sns
import pyspark
import pyspark.sql.functions as F
import re
import ast

conf=SparkContext("local[2]","NetworkWordCount")
sc=StreamingContext(conf,1)
#sqc=SQLContext(conf)
spark=SparkSession.builder.appName('SFCrimeClassification').getOrCreate()



def jsonToDf(rdd):
	if not rdd.isEmpty():
		cols=["Date","Category","Description","dayOfweek","District","Resolution","address","X","Y"]
		'''
		   cols = StructType([StructField("Date", StringType(), True),\
                                      StructField("Category", StringType(), True),\
                                      StructField("Description", StringType(), True),\
                                      StructField("DayOfWeek", StringType(), True),\
                                      StructField("District", StringType(), True),\
                                      StructField("Resolution", StringType(), True),\
                                      StructField("Address", StringType(), True),\
                                      StructField("X", StringType(), True),\
                                      StructField("Y", StringType(), True)
                                     ])
        '''
		df=spark.read.json(rdd)
		for row in df.rdd.toLocalIterator():
			'''
			r1=".".join(row)
			print(type(r1))
			r2=eval(r1)
			print(type(r2))
			#r2=r1.split("\\n")
			#print(type(r2))
			#result = [k.split(",") for k in r2]
			#print(result)
			'''
			for r in row:

				res=ast.literal_eval(r)
				row1=[]
				for line in res:
					line=re.sub('\\n','',line)
					line=re.sub(r',(?=[^"]*"(?:[^"]*"[^"]*")*[^"]*$)',"",line)
					line=re.sub('"',"",line)
					rowList=line.split(',')
					if not "Dates" in rowList:
						row1.append(rowList)

			df1=spark.createDataFrame(row1,schema=cols)
			df2 = df1.drop('Description','Resolution')
			df2.show(5)
			df2.columns
			df2.dtypes
			df2.select([count(when(isnull(c), c)).alias(c) for c in df2.columns]).show()
			split_col = pyspark.sql.functions.split(df2['Dates'],' ')
			df2 = df2.withColumn('Dates', split_col.getItem(0))
			df2 = df2.withColumn('Time', split_col.getItem(1))
			split_col = pyspark.sql.functions.split(df2['Date'],'-')
			df2 = df2.withColumn('Year', split_col.getItem(0))
			df2 = df2.withColumn('Month', split_col.getItem(1))
			df2 = df2.withColumn('Day', split_col.getItem(2))
			split_col = pyspark.sql.functions.split(df2['Time'],':')
			df2 = df2.withColumn('Hours', split_col.getItem(0))
			df2 = df2.withColumn('Minutes', split_col.getItem(1))
			df2 = df2.withColumn('Seconds', split_col.getItem(2))
			df2.show(5)
            
			
			

lines = sc.socketTextStream("localhost", 9000)
lines.foreachRDD(lambda rdd: jsonToDf(rdd))
sc.start()
sc.awaitTermination()
sc.stop(stopSparkContext=False) 






	


