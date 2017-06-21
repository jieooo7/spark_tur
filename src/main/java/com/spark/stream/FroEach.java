package com.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by thy on 17-6-20.
 */
public class FroEach {
	
	public static void main(String[] args)throws Exception{
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9989);
		
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(
				new FlatMapFunction<String, String>() {
					@Override public Iterator<String> call(String x) {
						return Arrays.asList(x.split(" ")).iterator();
					}
				});
		
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					@Override public Tuple2<String, Integer> call(String s) {
//						log.info("http://test\nkey:"+s);
						return new Tuple2<>(s, 1);
					}
				});
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					@Override public Integer call(Integer i1, Integer i2) {
//						log.info("http://test\nvalue:"+(i1+i2));
						return i1 + i2;
					}
				});
		
		
//		wordCounts.dstream().foreachRDD(new VoidFunction<JavaRDD<String>>() {
//			@Override
//			public void call(JavaRDD<String> rdd) {
//				rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
//					@Override
//					public void call(Iterator<String> partitionOfRecords) {
////						Connection connection = createNewConnection();
//						// ConnectionPool is a static, lazily initialized pool of connections
////						Connection connection = ConnectionPool.getConnection();
//						while (partitionOfRecords.hasNext()) {
////							connection.send(partitionOfRecords.next());
//						}
////						connection.close();
//						ConnectionPool.returnConnection(connection); // return to the pool for future reuse
//					}
//				});
//			}
//		});
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();
	}
}
