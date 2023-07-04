package com.delta.Demo;

import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App2
{
    public static void main( String[] args )
    {

        // 创建 SparkSession
        try (Scanner scanner = new Scanner(System.in);SparkSession spark = SparkSession
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
                // 调试时.show可以完整显示
                .config("spark.sql.repl.eagerEval.enabled", "true")
                .getOrCreate()) {
            spark.sql("create database if not exists test_sparkapp").collect();

            spark.sql("drop table if exists test_sparkapp.dli_testtable").collect();
            spark.sql("create table test_sparkapp.dli_testtable(id INT, name STRING) USING delta LOCATION 'file:///Users/dexterl/workspace/practice/deltapractice/demo01/spark-warehouse/delta_table2'").collect();
            // 引入一张外部表，将数据merge进目标表
            spark.sql("CREATE TABLE delta_table (id LONG, data STRING) USING delta LOCATION 'file:///Users/dexterl/workspace/practice/deltapractice/demo01/spark-warehouse/delta_table' ");

            while(true) {
                System.out.println("customized sql (exit input -1):");
                String sql = scanner.nextLine();
                if(sql.equals("-1")) {
                    break;
                }
                spark.sql(sql).show(false);
            }

            // 两条测试数据
            System.out.println("insert 2 rows to target table(y/n):");
            if(scanner.next().equals("y")) {
                spark.sql("insert into test_sparkapp.dli_testtable VALUES (123,'jason')").collect();
                spark.sql("insert into test_sparkapp.dli_testtable VALUES (456,'merry')").collect();
                spark.sql("DESCRIBE HISTORY test_sparkapp.dli_testtable").show(false);
            }

            // 写入数据到 Delta 表
            System.out.println("insert 5 rows to external table(y/n):");
            if(scanner.next().equals("y")) {
                for (int i = 0; i < 5; i++) {
                    spark.sql(String.format("INSERT INTO delta_table values (%d, '%s');", i, i));
                }
            }

            System.out.println("merge external table to target table(y/n):");
            if(scanner.next().equals("y")) {
                spark.sql("merge into test_sparkapp.dli_testtable as target using delta_table as source " +
                        "on source.id=target.id " +
                        "when not matched " +
                        "THEN INSERT (id, name) VALUES (source.id, source.data)").collect();

                spark.sql("SELECT * FROM test_sparkapp.dli_testtable").show();
            }

            while(true) {
                spark.sql("DESCRIBE HISTORY test_sparkapp.dli_testtable").show(false);
                // Printing the read line
                System.out.println("input time travel version(exit input -1):");
                Integer version = scanner.nextInt();
                if(version<0){
                    break;
                }
                spark.sql("SELECT * FROM test_sparkapp.dli_testtable VERSION AS OF " + version).show();
            }

            System.out.println("optimize the table");
            spark.sql("OPTIMIZE test_sparkapp.dli_testtable ZORDER BY (id)");
            while(true) {
                spark.sql("DESCRIBE HISTORY test_sparkapp.dli_testtable").show(false);
                // Printing the read line
                System.out.println("input time travel version(exit input -1):");
                Integer version = scanner.nextInt();
                if(version<0){
                    break;
                }
                spark.sql("SELECT * FROM test_sparkapp.dli_testtable VERSION AS OF " + version).show();
            }
        }
    }
}
