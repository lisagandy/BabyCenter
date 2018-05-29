from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql.functions import split,explode

#/opt/spark/bin/./pyspark --packages com.databricks:spark-xml_2.11:0.4.1

if __name__ == "__main__":
    
    conf = SparkConf().setMaster("local").setAppName("BabyCenterStats")
    sc = SparkContext(conf=conf)
    sqc = SQLContext(sc)
     
    dataframe = sqc.read.format("com.databricks.spark.xml").options(rowTag="bc_thread").load("JAN_2013")
    #dataframe.registerTempTable("bcData")
    #dataframe.printSchema()
    #dataframe.show()
    #dataframe.groupby('reply.date').agg({'reply.text':'count'}).show()
    #dfDateReply = dataframe.select('reply.text', \
		#explode('reply.date').alias('date'))
    #dfDateReply = dfDateReply.groupby('date').agg({'text':'count'})
    #dfDateReply.show()
    #dfDateReply.coalesce(1).write.format('com.databricks.spark.csv').save('SmallCounts2.csv')
    
    
