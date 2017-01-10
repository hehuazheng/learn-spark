package com.hzz.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("wordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> linesRdd = ctx.textFile(args[0], 1);
        linesRdd.cache();
        List<String> lines = linesRdd.collect();
        JavaRDD<String> wordsRdd = linesRdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                return Arrays.asList(words).iterator();
            }
        });
        JavaPairRDD<String, Integer> onesRdd = wordsRdd.mapToPair(new PairFunction<String, String, Integer>(){
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = onesRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        List<Tuple2<String, Integer>> output = counts.collect();
        for(Tuple2<String, Integer> t : output) {
            System.out.println(t._1 + ":" + t._2);
        }
        counts.saveAsTextFile("/tmp/counts.txt");
        ctx.stop();
        ctx.close();
    }
}
