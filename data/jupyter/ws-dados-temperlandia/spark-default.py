from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (SparkSession.builder
             .master("spark://spark-master:7077") # Points to the Spark Cluster
             .appName('lab') # Name the app
             .config("hive.metastore.uris", "thrift://hive-metastore:9083") # Set external Hive Metastore
             .config("hive.metastore.warehouse.dir", "s3a://minio:9000/datalake/") # Set default warehouse dir (legacy) users/hive/warehouse
             .config("spark.sql.warehouse.dir", "s3a://minio:9000/datalake/") # Set default warehouse dir
             .config("hive.metastore.schema.verification", "false") # Prevent some errors
             .config("fs.defaultFS", "s3a://minio:9000/datalake/") # Set default file system into the HDFS namenode
             .config("spark.jars", "/opt/bitnami/spark/jars_external/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars_external/aws-java-sdk-bundle-1.12.588.jar,/opt/bitnami/spark/jars_external/delta-core_2.12-2.4.0.jar,/opt/bitnami/spark/jars_external/delta-contribs_2.12-2.4.0.jar,/opt/bitnami/spark/jars_external/delta-storage-2.4.0.jar")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.debug.maxToStringFields", "100")
             .config("spark.sql.repl.eagerEval.enabled", True)
             .config("spark.driver.memory", "16g")  # Memória para o driver
             .config("spark.executor.memory", "32g")  # Memória para os executores
             .config("spark.executor.cores", "10")  # Número de núcleos por executor
             .enableHiveSupport()
             .getOrCreate())

sc = spark.sparkContext

hdp_configs = {
    "fs.s3a.endpoint": "http://minio:9000",
    "fs.s3a.access.key": "minio",
    "fs.s3a.secret.key": "minioadmin",
    "fs.s3a.connection.timeout": "600000",
    "spark.sql.debug.maxToStringFields": "100",
    "fs.s3a.path.style.access": "true",
    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "fs.s3a.connection.ssl.enabled": "true"
}

for k,v in hdp_configs.items():
    spark.sparkContext._jsc.hadoopConfiguration().set(k, v)