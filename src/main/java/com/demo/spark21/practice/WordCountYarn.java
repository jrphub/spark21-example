package com.demo.spark21.practice;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

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
public class WordCountYarn {
	public static void main(String[] args) throws IOException {
		String path = "/home/jrp/softwares/spark-2.1.0-bin-hadoop2.7/jars/";
		SparkConf sparkConf = new SparkConf()
				.setAppName("WordCount_21")
				.setMaster("yarn")
				.set("deploy-mode", "cluster")
				.set("spark.sql.hive.metastore.jars", "builtin")
				.setJars(
						new String[] { path + "spark-core_2.11-2.1.0.jar",
								path + "spark-launcher_2.11-2.1.0.jar",
								path + "spark-network-shuffle_2.11-2.1.0.jar",
								path + "spark-yarn_2.11-2.1.0.jar" });

		System.setProperty("HADOOP_USER_NAME", "huser");
		System.setProperty("SPARK_YARN_MODE", "yarn");
		/*
		 * SparkSession session = SparkSession.builder().config(sparkConf)
		 * .getOrCreate();
		 */
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<String> distFile = jsc.textFile("input-data/wordcount.txt");

		JavaRDD<String> flat_words = distFile
				.flatMap(new FlatMapFunction<String, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 2451133852416744577L;

					public Iterator<String> call(String line) throws Exception {
						return Arrays.asList(line.split(" ")).iterator();
					}
				});

		JavaPairRDD<String, Long> flat_words_mapped = flat_words
				.mapToPair(new PairFunction<String, String, Long>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = -1866004469677211524L;

					public Tuple2<String, Long> call(String flat_word)
							throws Exception {
						return new Tuple2<String, Long>(flat_word, 1L);
					}
				});

		JavaPairRDD<String, Long> flat_words_reduced = flat_words_mapped
				.reduceByKey(new Function2<Long, Long, Long>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = -867632747099075164L;

					public Long call(Long l1, Long l2) throws Exception {
						return l1 + l2;
					}
				});

		// FileUtils.deleteDirectory(new File("output/wordcount_output"));
		flat_words_reduced.saveAsTextFile("output/wordcount_output");

		// UserGroupInformation ugi =
		// UserGroupInformation.getLoginUser().doAs("hduser");

		jsc.close();
	}
}
