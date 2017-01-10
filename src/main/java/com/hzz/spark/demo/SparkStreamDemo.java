package com.hzz.spark.demo;


import com.google.common.collect.Sets;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class SparkStreamDemo {
    /**
     * # ./bin/spark-submit --class com.hzz.spark.demo.SparkStreamDemo --jars /root/spark-streaming-kafka_2.10-1.6.3.jar,/root/spark-streaming_2.10-2.0.2.jar,/root/kafka_2.10-0.8.2.1.jar,/root/scala-library-2.10.6.jar,/root/metrics-core-2.2.0.jar /root/spark-hive-demo-1.0-SNAPSHOT.jar
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Duration durationWindow = Durations.seconds(3);
        SparkConf sparkConf = new SparkConf().setAppName("sparkStream");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, durationWindow);
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("zookeeper.sync.time.ms", "200");
        kafkaParams.put("enable.auto.commit", "true");
        kafkaParams.put("group.id", "testGroup");
        JavaPairInputDStream<String, String> jpis = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class
                , StringDecoder.class, kafkaParams, Sets.newHashSet("testTopic"));
        JavaDStream<String> res = jpis.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println("processing:" + stringStringTuple2._1 + "%" + stringStringTuple2._2);
                return stringStringTuple2._1 + "#" + stringStringTuple2._2;
            }
        });
        res.window(durationWindow).foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.collect();
            }
        });
        jsc.start();
        jsc.awaitTermination();
    }
}
