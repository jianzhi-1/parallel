import pyspark
import time

def square(x):
    time.sleep(5)
    return x*x

sc = pyspark.SparkContext('local[*]') # connection to spark cluster
R = sc.parallelize(range(4))
print(R.map(square).collect())
