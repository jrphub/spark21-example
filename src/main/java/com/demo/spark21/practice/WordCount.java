package com.demo.spark21.practice;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Word Count using Spark 2.1
 *
 */
public class WordCount {
	public static void main(String[] args) throws IOException {
		SparkConf sparkConf = new SparkConf().setAppName("WordCount_21")
				.setMaster("local[*]");
		
		/*
		 * SparkSession session = SparkSession.builder().config(sparkConf)
		 * .getOrCreate();
		 */
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<String> distFile = jsc.textFile("file:///home/jrp/workspace_1/Spark21-Example/input-data/wordcount.txt");

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
		
		//As the output file is in local, deleting output_dir using FileUtils
		FileUtils.deleteDirectory(new File("/home/jrp/workspace_1/Spark21-Example/output/wordcount_output"));
		//All spark job needs file:// or hdfs:// prefix to distinguish between local and cluster
		flat_words_reduced.saveAsTextFile("file:///home/jrp/workspace_1/Spark21-Example/output/wordcount_output");

		jsc.close();
	}
}
