package com.demo.spark21.practice;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Word Count using Spark 2.1
 *
 */
public class WordCountYarn {
	public static void main(String[] args) throws IOException {
		SparkConf sparkConf = new SparkConf().setAppName("WordCountYarn")
				.setMaster("yarn")
				.set("deploy-mode", "client")
				.set("spark.executor.instances", "2")
				.set("spark.dynamicAllocation.enabled", "true")
				.set("spark.shuffle.service.enabled", "true")
				.set("spark.dynamicAllocation.minExecutors", "2")
				.set("spark.dynamicAllocation.maxExecutors", "4")
				.set("spark.yarn.shuffle.stopOnFailure", "true")
				.set("spark.default.parallelism", "4")
				.set("spark.sql.shuffle.partitions", "4")
				// reduce task
				.set("spark.serializer",
						"org.apache.spark.serializer.KryoSerializer");

		System.setProperty("HADOOP_USER_NAME", "huser");

		SparkSession session = SparkSession.builder().config(sparkConf)
				.getOrCreate();
		// session.sparkContext().addFile(path, false);;

		JavaRDD<String> distFile = session
				.read()
				.textFile(
						"file:///home/jrp/workspace_1/Spark21-Example/input-data/wordcount.txt")
				.javaRDD();

		JavaRDD<String> flat_words = distFile
				.flatMap(new FlatMapFunction<String, String>() {
					
					public Iterator<String> call(String line) throws Exception {
						return Arrays.asList(line.split(" ")).iterator();
					}
				});
		
		JavaPairRDD<String, Long> flat_words_mapped = flat_words
				.mapToPair(new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String flat_word)
							throws Exception {
						return new Tuple2<String, Long>(flat_word, 1L);
					}
				});

		JavaPairRDD<String, Long> flat_words_reduced = flat_words_mapped
				.reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long l1, Long l2) throws Exception {
						return l1 + l2;
					}
				});

		System.out.println(flat_words_reduced.collectAsMap().size());
		//System.out.println(distFile.collect().size());
	}
}
