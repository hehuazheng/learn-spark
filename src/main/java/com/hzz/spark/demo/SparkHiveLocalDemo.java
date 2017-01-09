package com.hzz.spark.demo;

import org.apache.spark.sql.SparkSession;

/**
 * Created by hejf on 2017/1/6.
 */
public class SparkHiveLocalDemo {
    public static void main(String[] args) {
        System.out.println("args:"+args);
        String warehouseLocation = args[0];
        SparkSession spark = SparkSession.builder().appName("java spark hive example").config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport().getOrCreate();
        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        spark.sql("LOAD DATA LOCAL INPATH '"+args[1]+"' INTO TABLE src");
        spark.sql("SELECT * FROM src").show();
        spark.stop();
    }
}
