package com.hzz.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by hejf on 2017/1/6.
 */
public class BigFileTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("BigFileTest");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> linesRdd = ctx.textFile(args[0], 1);
        JavaRDD<String> linesRdd2 = ctx.textFile(args[1], 1);
        linesRdd = linesRdd.union(linesRdd2);
        System.out.println("文件行数：" + linesRdd.collect().size());
        ctx.stop();
        ctx.close();
    }
}
