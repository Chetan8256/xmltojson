package com.xmltojson


import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions.udf


object SparkHbase {
    def main(args : Array[String]) {
        
        println( "Hello World!" )

	    val conf = new SparkConf().setAppName("XmlToJSON").setMaster("spark://127.0.0.1:7077")
	    val sc = new SparkContext(conf)
	    val spark = SparkSession
			.builder()
			.appName("XmlToJson")
			.getOrCreate()


	    // For implicit conversions like converting RDDs to DataFrames
	    import spark.implicits._
	    //val content = sc.textFile("hdfs://127.0.0.1:9000/user/cruise/input/data2.txt", 10)

	    val encrypt: String => String = (str: String) => {
		    val md = java.security.MessageDigest.getInstance("SHA-1")
		    md.digest(str.getBytes("UTF-8")).map("%02x".format(_)).mkString
	    }
	    val encryptUDF = udf(encrypt)


	    //var df = spark.read.format("com.databricks.spark.xml").option("rowTag", "eventData").load("hdfs://127.0.0.1:9000/user/cruise/TestData3.xml")
	    var df = spark.read.format("com.databricks.spark.xml").option("rowTag", "eventData").load("hdfs://127.0.0.1:9000/user/cruise/TestData3.xml")
	    //df.write.mode("append").parquet("hdfs://127.0.0.1:9000/user/cruise/input/TestData2_original.parquet")

	    df = df.select(
		    $"net._id".alias("network_id"),
		    $"net._name".alias("network_name"),
		    explode($"net.event")
	    );
	    df.printSchema()

	    @transient val affected_lanes = struct(
		    $"col.affectedLanes._lane1".as("lane"), $"col.affectedLanes._VALUE".as("lane_status")
	    ).alias("affected_lanes")
	    @transient val message_codes = struct(
		    $"col.messageCodes.code1".as("code1"), $"col.messageCodes.code1".as("code4")
	    ).alias("message_codes")

	    @transient val location = struct(
		    $"col.primaryLoc.lat".as("lat"), $"col.primaryLoc.lon".as("long")
	    ).alias("location")
	    val newdf = df.select(
		    encryptUDF($"col._id").alias("doc_id"),
		    $"network_id",
		    $"network_name",
		    $"col.updateTimestamp".alias("update_timestamp"),
		    $"col._id".alias("event_id"),
		    $"col.severity".alias("severity"),
		    $"col.updateType".alias("update_type"),
		    $"col.primaryLoc.locationId".alias("location_id"),
		    location,
		    $"col.status".alias("status"),
		    $"col.source".alias("source"),
		    $"col.contact".alias("contact"),
		    message_codes,
		    affected_lanes,
		    $"col.type._VALUE".alias("type"),
		    $"col.typeDesc".alias("type_description"),
		    $"col.roadCond._VALUE".alias("road_conditions"),
		    $"col.weatherCond._VALUE".alias("weather_conditions"),
		    $"col.startTimestamp".alias("start_timestamp"),
		    $"col.atisSeverity.severity".alias("atis_severity"),
		    $"col.atisSeverity.timestamp".alias("atis_timestamp")
	    )

	    newdf.write.mode("append").csv("hdfs://127.0.0.1:9000/user/cruise/TestData3")
	    //val hbaseconf = HBaseConfiguration.create()

	    //val jobConfig = new JobConf(hbaseconf, this.getClass)


    }
}