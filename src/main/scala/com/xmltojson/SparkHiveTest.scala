/**
  * Created by cruise on 4/1/18.
  */

package com.xmltojson


import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.immutable.List
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.xml._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.databricks.spark.xml.XmlReader
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode


object SparkHiveTest {
	def main(args : Array[String]) {

		println( "Hello World!" )

		val conf = new SparkConf().setAppName("XmlToJSON").setMaster("spark://127.0.0.1:7077")
		val sc = new SparkContext(conf)
		val spark = SparkSession
		  .builder()
		  .enableHiveSupport()
		  .config("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
		  .appName("XmlToJson")
		  .getOrCreate()


		// For implicit conversions like converting RDDs to DataFrames
		import spark.implicits._
		//val content = sc.textFile("hdfs://127.0.0.1:9000/user/cruise/input/data2.txt", 10)

		//var df = spark.read.format("com.databricks.spark.xml").option("rowTag", "eventData").load("hdfs://127.0.0.1:9000/user/cruise/TestData3.xml")
		var df = spark.read.format("com.databricks.spark.xml").option("rowTag", "eventData").load("hdfs://127.0.0.1:9000/user/cruise/TestData3.xml")
		//df.write.mode("append").parquet("hdfs://127.0.0.1:9000/user/cruise/input/TestData2_original.parquet")

		df = df.select(
			$"net._id".alias("network_id"),
			$"net._name".alias("network_name"),
			explode($"net.event")
		);
		
		df.printSchema()

		df.createOrReplaceTempView("events")
		if (spark.sql("show tables like 'ng_events'").collect().length == 1) {
		    println("ng_events table is already exists")
		    spark.sql("insert into ng_events select * from events")
		} else {
		    spark.sql("create table IF NOT EXISTS ng_events as select * from events")
		}



	}
}