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


object SparkHive {
    def main(args : Array[String]) {
        
        println( "Hello World!" )
        
        val conf = new SparkConf().setAppName("XmlToJSON").setMaster("spark://127.0.0.1:7077")
        .set("spark.driver.allowMultipleContexts", "true")
        .set("spark.driver.cores", "2")
        
        val sc = new SparkContext(conf)
                
        val ssc = new StreamingContext(sc, Seconds(30))
        println("************************start stremaing *************************.")    
        val xmlDStream = ssc.textFileStream("hdfs://127.0.0.1:9000/user/cruise/streaming")
        println("************************ create DStream *************************.")

        
        xmlDStream.foreachRDD { xmlRdd =>
            // Get the singleton instance of SparkSession
            val spark = SparkSession.builder
            .enableHiveSupport()
            .config("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
            .config(xmlRdd.sparkContext.getConf).getOrCreate()
            import spark.implicits._
            println("let me check. ==  = = =   "  + xmlRdd.collect().length + "    ---------------")

            
            val fields = Seq("event_id", "network_name","_updateTime", "xmlns", "affected_lanes", "atis_severity", "category", "contact", "message_codes", "location","road_conditions", "secondaryLoc", "severity", "source", "start_timestamp", "status","type", "type_description", "update_timestamp", "update_type", "weather_conditions")
            
            if(xmlRdd.collect().length > 0){
                
                val rdd = ssc.sparkContext.parallelize(Seq(xmlRdd.collect().mkString("")))
                var df = new XmlReader().xmlRdd(spark.sqlContext, rdd)
                df.printSchema()
                
                df = df.select(
                    $"eventData.net._id".alias("network_id"), 
                    $"eventData.net._name".alias("network_name"), 
                    explode($"eventData.net.event")
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
                
                //newdf.write.mode("append").saveAsTable("%s.%s".format( "default" , "events" ))
                newdf.createOrReplaceTempView("events")
                
                 if (spark.sql("show tables like 'ng_events'").collect().length == 1) {
              		    println("ng_events table is already exists")
              		    spark.sql("insert into ng_events select * from events")
                 } else {
              		    spark.sql("create table IF NOT EXISTS ng_events as select * from events")
                	}
                //newdf.select("network_id", "network_name").write.mode("overwrite").saveAsTable("events")
                
            } else {
                println("~~~~~~~~~~~~~~~~~~No New data found~~~~~~~~~~~~~~~~~~~")
            }
        }

        println()
        println("------------------------ printing DStreamig -------------------------")
        xmlDStream.print()
        println(" --------------------printing DStreamig&&&&&&&&&&&")
        ssc.start()
        ssc.awaitTermination()
        
        
    }
}