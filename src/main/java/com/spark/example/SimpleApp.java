package com.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

public class SimpleApp {
	public static void main(String[] args) {
		String logFile = "/home/thy/spark-2.1.1-bin-hadoop2.7/README.md"; // Should be some file on your system
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		conf.setMaster("local[4]");//./bin/spark-shell --master local[2] 启动spark
		//实际中,不需要设置master,测试的时候指定为"local"
		JavaSparkContext sc = new JavaSparkContext(conf);
//		In the Spark shell, a special interpreter-aware SparkContext is already created for you, in the variable called sc.
// Making your own SparkContext will not work. 自己指定的不一定生效,shell自适应的
//		sc.addJar("/home/thy/IdeaProjects/qht/tur_spark/target/spark-1.0-SNAPSHOT.jar");
		JavaRDD<String> logData = sc.textFile(logFile).cache();
		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("a"); }
		}).count();
		
		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("b"); }
		}).count();
		
		List<String> list=logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("a"); }
		}).collect();
		
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		for(String s:list){
			
			System.out.println("List: " +s);
		}
		try {
			Thread.currentThread().sleep(1000000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		sc.stop();
	}
}



//./bin/spark-shell --help

//# Package a JAR containing your application
//		$ mvn package
//		...
//		[INFO] Building jar: {..}/{..}/target/simple-project-1.0.jar
//
//		# Use spark-submit to run your application
//		$ YOUR_SPARK_HOME/bin/spark-submit \
//		--class "SimpleApp" \
//		--master local[4] \
//		target/simple-project-1.0.jar
//		...
//		Lines with a: 46, Lines with b: 23




//# For Scala and Java, use run-example:
//		./bin/run-example SparkPi
//
//		# For Python examples, use spark-submit directly:
//		./bin/spark-submit examples/src/main/python/pi.py
//
//		# For R examples, use spark-submit directly:
//		./b in/spark-submit examples/src/main/r/dataframe.R

//./bin/pyspark  可以直接进入Python交互式编程
