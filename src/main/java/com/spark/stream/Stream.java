package com.spark.stream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
 * Created by thy on 17-6-19.
 */
public class Stream {
	private static final transient Logger log = LogManager.getLogger(Stream.class);
	//找不到包在模块依赖中设置为complile
//	        * compile，缺省值，适用于所有阶段，会随着项目一起发布。
//			* provided，类似compile，期望JDK、容器或使用者会提供这个依赖。如servlet.jar。
//			* runtime，只在运行时使用，如JDBC驱动，适用运行和测试阶段。
//			* test，只在测试时使用，用于编译和运行测试代码。不会随项目发布。
//			* system，类似provided，需要显式提供包含依赖的jar，Maven不会在Repository中查找它。
	public static void main(String[] agrs) throws Exception{
//		local 单线程的,建议使用local n n根据reciver的个数来确定
		// Create a local StreamingContext with two working thread and batch interval of 1 second
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
// Print the first ten elements of each RDD generated in this DStream to the console
//		wordCounts.print();//不能和saveas*** 同时使用
		wordCounts.dstream().saveAsTextFiles("file:///home/thy/logs/log","txt");
//		wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
//			@Override
//			public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
//
//			}
//		});
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
		
//		The processing can be manually stopped using streamingContext.stop().
//		stop() on StreamingContext also stops the SparkContext.
// To stop only the StreamingContext, set the optional parameter of stop()
// called stopSparkContext to false.
	}
}

//从文件,hdfs读入
//streamingContext.fileStream<KeyClass, ValueClass, InputFormatClass>(dataDirectory);
	
	/**
	 *
	 Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
	 new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
	@Override public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
	Integer newSum = ...  // add the new values with the previous running count to get the new count
	return Optional.of(newSum);
	}
	};
	 
	 
	 JavaPairDStream<String, Integer> runningCounts = pairs.updateStateByKey(updateFunction);
	 
	 
	 
	 
	 
	 JavaPairDStream<String, Integer> cleanedDStream = wordCounts.transform(
	 new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
	@Override public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> rdd) throws Exception {
	rdd.join(spamInfoRDD).filter(...); // join data stream with spam information to do data cleaning
	...
	}
	});
	 
	 
	 // Reduce function adding two integers, defined separately for clarity
	 Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
	@Override public Integer call(Integer i1, Integer i2) {
	return i1 + i2;
	}
	};
	 
	 // Reduce last 30 seconds of data, every 10 seconds
	 JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow(reduceFunc, Durations.seconds(30), Durations.seconds(10));
	 *
	 *
	 */
	