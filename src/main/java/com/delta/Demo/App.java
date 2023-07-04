package com.delta.Demo;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        // 创建 SparkSession
        try (SparkSession spark = SparkSession
                .builder()
                .appName("Delta Experiment")
                .master("local[*]")
                //.master("spark://raspberry.local:7077")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // 设置资源
                //.config("spark.task.cpus", "2")
                // 设置 Delta Lake 相关配置
                .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                .config("spark.driver.log.level", "WARN")
                .config("spark.executor.log.level", "WARN")
                .getOrCreate()) {

            // 在这里编写您的 Delta 表操作代码
            // 创建 Delta 表
            spark.sql("CREATE TABLE delta_table (id LONG, data STRING) USING delta LOCATION 'file:///Users/dexterl/workspace/practice/deltapractice/demo01/spark-warehouse/delta_table' ");
            // 查询 Delta 表
            spark.sql("SELECT * FROM delta_table").show();
            // 写入数据到 Delta 表
//            for (int i = 0; i < 5; i++) {
//                spark.sql(String.format( "INSERT INTO delta.`/Users/dexterl/workspace/practice/deltapractice/demo01/spark-warehouse/delta_table` values (%d, '%s');",i, i));
//            }

            spark.sql("DESCRIBE HISTORY delta.`/Users/dexterl/workspace/practice/deltapractice/demo01/spark-warehouse/delta_table`").show();
            // 查询 Delta 表
            spark.sql("SELECT * FROM delta.`/Users/dexterl/workspace/practice/deltapractice/demo01/spark-warehouse/delta_table` VERSION AS OF 0").show();
            spark.sql("SELECT * FROM delta.`/Users/dexterl/workspace/practice/deltapractice/demo01/spark-warehouse/delta_table` VERSION AS OF 2").show();

            spark.sql("SELECT * FROM delta.`/Users/dexterl/workspace/practice/deltapractice/demo01/spark-warehouse/delta_table`").show();
        }
    }
}
