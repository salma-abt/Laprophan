import os
import sys
import socket
from pyspark.sql import SparkSession

print("Python Version:", sys.version)
print("Java Home:", os.environ.get("JAVA_HOME"))
print("Hadoop Home:", os.environ.get("HADOOP_HOME"))
print("Hostname:", socket.gethostname())

try:
    print("Creating Spark Session (Networking stabilization)...")
    s = SparkSession.builder.master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.port", "4040") \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()
    print(f"Spark Version: {s.version}")
    
    print("Creating DataFrame...")
    df = s.createDataFrame([(i,) for i in range(10)], ["id"])
    
    print("Executing Count...")
    count = df.count()
    print(f"Count: {count}")
    
    s.stop()
    print("DONE: SUCCESS")
except Exception as e:
    print(f"DONE: ERROR: {e}")
    import traceback
    traceback.print_exc()
