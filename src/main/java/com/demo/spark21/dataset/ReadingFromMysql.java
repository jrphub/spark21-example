package com.demo.spark21.dataset;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadingFromMysql {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"Reading From Mysql");

		SparkSession session = SparkSession.builder().config(conf)
				.getOrCreate();

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "admin");

		String url = "jdbc:mysql://localhost:3306/testdb";

		/*
		 * Dataset<Row> jdbcDF = session.read().format("jdbc") .option("url",
		 * "jdbc:mysql://localhost:3306/testdb") .option("dbtable", "movies")
		 * .option("user", "root") .option("password", "admin") .load();
		 */

		Dataset<Row> jdbcDFMovies = session.read().jdbc(url, "movies",
				connectionProperties);

		jdbcDFMovies.show();

		Dataset<Row> jdbcDFRatings = session.read().jdbc(url, "ratings",
				connectionProperties);

		Dataset<Row> movies_ratings = jdbcDFMovies.join(jdbcDFRatings,
				"movieid");
		
		// System.out.println(movies_ratings.collectAsList().size());
		
		Dataset<Row> jdbcDFUsers = session.read().jdbc(url, "users",
				connectionProperties);

		

	}
}
