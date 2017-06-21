package com.spark.example;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * Created by thy on 17-6-16.
 */
public class LearnNote {
//	private static Log log = new Log(LoggerFactory.getLogger(LearnNote.class));
	private static final transient Logger log = LogManager.getLogger(LearnNote.class);
	public static void main(String[] args){
//		Resilient Distributed Datasets (RDDs)
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		conf.setMaster("local");//./bin/spark-shell --master local[2] 启动spark
		//实际中,不需要设置master,测试的时候指定为"local"
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);//并行化处理,分片处理,并行运算
		//textfile支持目录,url,打包文件,通配符
		JavaRDD<String> distFile = sc.textFile("/home/thy/IdeaProjects/qht/tur_spark/src/main/resources/data.txt");// This method takes an URI for the file (either a local path on the machine, or a hdfs://, s3n://, etc URI) and reads it as a collection of lines.
//		distFile.map(s -> s.length()).reduce((a, b) -> a + b).
		JavaRDD<Integer> lineLengths = distFile.map(new Function<String, Integer>() {
			public Integer call(String v1) throws Exception {
				log.info("http://test\n元素:"+v1);
				return v1.length();
			}
		});
		//map做一次转换
//		If we also wanted to use lineLengths again later, we could add:
//		lineLengths.persist(StorageLevel.MEMORY_ONLY());
//
//		before the reduce, which would cause lineLengths to be saved in memory after the first time it is computed.
		
		
		
		Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
//		broadcast variables and accumulators. 共享变量
		broadcastVar.value();
// returns [1, 2, 3]
		
		
		LongAccumulator accum = sc.sc().longAccumulator();
		
		sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> {accum.add(x);log.info("http://test\nsum:"+x);});
		
		accum.value();
		
		int sumFile=distFile.map(new Function<String, Integer>() {
			public Integer call(String v1) throws Exception {
				log.info("http://test\n元素:"+v1);
				return Integer.parseInt(v1);
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		int sum=distData.reduce(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		}).intValue();
	log.info("http://test\nsum:"+sum);
	log.info("http://test\nsumFile:"+sumFile);
		sc.stop();
		
//		Launching Spark jobs from Java / Scala
//		SparkAppHandle handle = new SparkLauncher()
//				.setAppResource("/my/app.jar")
//				.setMainClass("my.spark.app.Main")
//				.setMaster("local")
//				.setConf(SparkLauncher.DRIVER_MEMORY, "2g")
//				.startApplication();
		// Use handle API to monitor / control application.
		
		
//		Process spark = new SparkLauncher()
//				.setAppResource("/my/app.jar")
//				.setMainClass("my.spark.app.Main")
//				.setMaster("local")
//				.setConf(SparkLauncher.DRIVER_MEMORY, "2g")
//				.launch();
//		spark.waitFor();
	}
}
