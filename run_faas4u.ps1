$env:JAVA_HOME = "C:\Users\H P\.jdks\ms-17.0.14"
$env:HADOOP_HOME = "C:\hadoop"
$env:PYSPARK_PYTHON = "C:\Python313\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Python313\python.exe"

# Add Java and Hadoop to Path
$java_bin = "$env:JAVA_HOME\bin"
$hadoop_bin = "$env:HADOOP_HOME\bin"
$python_path = "C:\Python313"
$python_scripts = "C:\Python313\Scripts"

$env:Path = "$java_bin;$hadoop_bin;$python_path;$python_scripts;C:\Windows\System32;C:\Windows"

Write-Host "--- FAAS4U Environment Setup ---"
Write-Host "JAVA_HOME: $env:JAVA_HOME"
Write-Host "HADOOP_HOME: $env:HADOOP_HOME"
Write-Host "--- Running Validation Engine ---"

python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[1]').config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1').config('spark.python.worker.faulthandler.enabled', 'true').getOrCreate(); print('--- Engine Spark Init OK ---'); s.stop()"

python Notebooks/03_Validation_Engine.py
