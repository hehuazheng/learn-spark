package com.hzz.spark.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkHiveDemo {
    /**
     * # ./bin/spark-submit --driver-class-path /opt/hive-2.1.0/lib/mysql-connector-java-5.1.27.jar
     *  --class com.hzz.spark.demo.SparkHiveDemo /root/spark-hive-demo-1.0-SNAPSHOT.jar
     *  hdfs://localhost:9000//user/hive/warehouse/src/data.txt
     *
     * @param args
     */
    public static void main(String[] args) {
        String warehouseLocation = args[0];
        SparkSession spark = SparkSession.builder().appName("java spark hive example").config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport().getOrCreate();
        Dataset<String> ds = spark.read().textFile(args[0]);
        ds.show();
        spark.stop();
    }
}
