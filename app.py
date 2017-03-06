from pyspark import SparkContext, SparkConf

from pyspark.sql import Row, SparkSession

from pyspark.sql.types import *

sc = SparkContext()

spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.createDataFrame(sc.parallelize(range(1, 100))
        .map(lambda i: Row(single=i, square=i ** 2)))

df.write.format("orc").save("hdfs://10.20.30.101:8020/tmp/squares", mode='overwrite')
