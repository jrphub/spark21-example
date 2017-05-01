package com.demo.spark21.dataset;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadingFromMysql {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("yarn")
				.setAppName("Reading From Mysql")
				.set("deploy-mode", "client")
				.set("spark.executor.instances", "2")
				.set("spark.dynamicAllocation.enabled", "true")
				.set("spark.shuffle.service.enabled", "true")
				.set("spark.dynamicAllocation.minExecutors", "2")
				.set("spark.dynamicAllocation.maxExecutors", "4")
				.set("spark.yarn.shuffle.stopOnFailure", "true")
				.set("spark.default.parallelism", "4")
				.set("spark.sql.shuffle.partitions", "4") //reduce task
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		SparkSession session = SparkSession.builder().config(conf)
				.getOrCreate();

		session.sparkContext()
				.addJar("hdfs:///user/huser/jars/mysql/mysql-connector-java-5.1.6-bin.jar");
		
		System.out.println("=======\n" + session.conf().getAll() + "=======\n");

		System.setProperty("HADOOP_USER_NAME", "huser");

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "admin");

		String url = "jdbc:mysql://localhost:3306/testdb";
		
		Dataset<Row> jdbcDFUsers = session.read().jdbc(url, "users",
				connectionProperties);
		
		Dataset<Row> jdbcDFMovies = session.read().jdbc(url, "movies",
				connectionProperties);

		Dataset<Row> jdbcDFRatings = session.read().jdbc(url, "ratings",
				connectionProperties);

		Dataset<Row> moviesRatings = jdbcDFMovies
				.join(jdbcDFRatings, "movieid");
		
		moviesRatings.show();

		//jdbcDFMovies.javaRDD().saveAsTextFile("/user/huser/movies");

	}
}
