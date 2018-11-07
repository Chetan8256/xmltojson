package com.xmltojson

/**
  * @author ${user.name}
  */

import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.immutable.List
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.xml._

object SparkMysql {

	def main(args : Array[String]) {
		println( "Hello World!" )

		val conf = new SparkConf().setAppName("XmlToJSON").setMaster("spark://127.0.0.1:7077")
		val sc = new SparkContext(conf)
		val spark = SparkSession
		  .builder()
		  .appName("XmlToJson")
		  .getOrCreate()


		// For implicit conversions like convertinTopicPartitiong RDDs to DataFrames
		import spark.implicits._
		//val content = sc.textFile("hdfs://127.0.0.1:9000/user/cruise/input/data2.txt", 10)

		//var df = spark.read.format("com.databricks.spark.xml").option("rowTag", "eventData").load("hdfs://127.0.0.1:9000/user/cruise/TestData3.xml")
		var df = spark.read.format("jdbc")
		  .option("driver", "com.mysql.jdbc.Driver")
		  .option("url", "jdbc:mysql://localhost:3306/cellar")
		  .option("user", "root")
		  .option("password", "root")
		  .option("dbtable", "wine")
		  .load()

		//df.write.mode("append").parquet("hdfs://127.0.0.1:9000/user/cruise/input/TestData2_original.parquet")
		/*
		df = df.select(
			$"net._id".alias("network_id"),
			$"net._name".alias("network_name"),
			explode($"net.event")
		)*/
		df.printSchema()

		df.show()

	}
}
