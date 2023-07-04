bin/spark-sql --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

./bin/spark-shell --master "local[*]"  --packages io.delta:delta-core_2.12:1.1.0
```scala
import org.apache.spark.sql.SparkSession
import io.delta.tables._
val spark = SparkSession.builder().appName("Delta shell exp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.databricks.delta.retentionDurationCheck.enabled", "false").getOrCreate()
spark.sql("CREATE TABLE delta_table (id LONG, data STRING) USING delta LOCATION 'file:///path/to/demo01/spark-warehouse/delta_table'")
spark.sql("show databases").show()
spark.sql("show tables").show()
spark.sql("select * from delta_table").show()
```
