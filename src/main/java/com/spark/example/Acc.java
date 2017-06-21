package com.spark.example;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/**
 * Created by thy on 17-6-19.
 */
public class Acc {
	private static final transient Logger log = LogManager.getLogger(Acc.class);
	public static void main(String[] args){
//		Resilient Distributed Datasets (RDDs)
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		conf.setMaster("local");//./bin/spark-shell --master local[2] 启动spark
		//实际中,不需要设置master,测试的时候指定为"local"
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		LongAccumulator accum = sc.sc().longAccumulator();
		
		sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> {accum.add(x);log.info("http://test\nxxx:"+x);});
		
		log.info("http://test\nvalue:"+accum.value());
		sc.stop();
	}
}
