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



import org.apache.spark.sql.functions.udf

object App {
  
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
        var df = spark.read.format("com.databricks.spark.xml").option("rowTag", "eventData").load("hdfs://127.0.0.1:9000/user/cruise/TestData3.xml")
        df.write.mode("append").parquet("hdfs://127.0.0.1:9000/user/cruise/input/TestData2_original.parquet")
        
        df = df.select(
            $"net._id".alias("network_id"), 
            $"net._name".alias("network_name"), 
            explode($"net.event")
        );    
        df.printSchema()
        
        val encrypt: String => String = (str: String) => {
            val md = java.security.MessageDigest.getInstance("SHA-1")
            md.digest(str.getBytes("UTF-8")).map("%02x".format(_)).mkString
        }


        
        val encryptUDF = udf(encrypt)
        
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
        
        
        //val dataframe = newdf.withColumn("doc_id", encryptUDF(col("event_id")))
        //df.rdd.collect().foreach(println)  
        //dataframe.printSchema()
        //dataframe.show(5)
        //println(content.first)
        
        //val jsonrdd = sc.parallelize(Seq(pretty(render(jsonContent))))
/*        
        val fields = Seq("event_id", "network_id","_updateTime", "xmlns", "affected_lanes", "atis_severity", "category", "contact", "message_codes", "location","road_conditions", "secondaryLoc", "severity", "source", "start_timestamp", "status","type", "type_description", "update_timestamp", "update_type", "weather_conditions")
        val dfRenamed = df.toDF(fields: _*)
        
        dfRenamed.printSchema()
        
        @transient val affected_lanes = struct(
                $"affected_lanes._lane1".as("lane"), $"affected_lanes._VALUE".as("lane_status")
            ).alias("affected_lanes")
        
        @transient val message_codes = struct(
                $"message_codes.code1".as("code1"), $"message_codes.code1".as("code4")
            ).alias("message_codes")
            
        @transient val location = struct(
                $"location.lat".as("lat"), $"location.lon".as("long")
            ).alias("location")    
        
        @transient val atis_severity = $"atis_severity.severity".as("atis_severity")
        @transient val atis_timestamp = $"atis_severity.timestamp".as("atis_timestamp")
        //@transient val event_id = $"event_id._id".as("event_id")
        @transient val road_conditions = $"road_conditions._VALUE".as("road_conditions")
        @transient val typefield = $"type._VALUE".as("type")
        @transient val weather_conditions = $"weather_conditions._VALUE".as("weather_conditions")
        @transient val location_id = $"location.locationId".as("location_id")
        
        
//        val newDf = dfRenamed.select(affected_lanes, message_codes, $"network_name",atis_severity,atis_timestamp,$"category", $"contact",$"event_id",location,$"severity", $"source", $"start_timestamp", $"status" ,typefield, $"update_timestamp", $"update_type",weather_conditions,location_id )
  		val newDf = dfRenamed.select($"network_id",$"update_timestamp",$"event_id",$"severity",$"update_type",location_id,location,$"status",$"source",$"contact",message_codes,affected_lanes,typefield,weather_conditions,$"start_timestamp",atis_severity,atis_timestamp )      
  		dfRenamed.show(5)
  		*/
        //newDf.write.mode("append").json("hdfs://127.0.0.1:9000/user/cruise/input/TestData3")
        newdf.show(10)
        newdf.write.mode("overwrite").json("hdfs://127.0.0.1:9000/user/cruise/input/TestData2.json")
        /*
        val schema_map = s"""{"type": "map", "values": "int"}""".stripMargin
        newdf.write.options(
                  Map("schema_map" -> schema_map, HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
                  .format("org.apache.spark.sql.execution.datasources.hbase")
                  .save()
        */
        //newDf.toJSON.take(10).foreach(println)
        //newdf.toJSON.saveAsTextFile("hdfs://127.0.0.1:9000/user/cruise/input/nadia_output_new.json")
        
        
    }
}
