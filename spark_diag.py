import os
import sys
from pyspark.sql import SparkSession

print("Python Version:", sys.version)
print("Java Home:", os.environ.get("JAVA_HOME"))
print("Hadoop Home:", os.environ.get("HADOOP_HOME"))

try:
    print("Creating Spark Session (PySpark 3.5.3 mode)...")
    s = SparkSession.builder.master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.python.worker.reuse", "true") \
        .getOrCreate()
    print(f"Spark Version: {s.version}")
    
    print("Creating DataFrame...")
    df = s.createDataFrame([(1,)], ["id"])
    
    print("Executing Count...")
    # Using a simple aggregation to force a job
    count = df.count()
    print(f"Count: {count}")
    
    s.stop()
    print("DONE: SUCCESS")
except Exception as e:
    print(f"DONE: ERROR: {e}")
    import traceback
    traceback.print_exc()
