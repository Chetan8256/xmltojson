package com.xmltojson


import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.immutable.List
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.xml._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.json4s.Xml.{toJson}
import com.databricks.spark.xml.XmlReader
import org.apache.spark.sql.SQLContext

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.rdd.EsSpark

object SparkES {
    def main(args : Array[String]) {
        println( "Hello World!" )
        
        val conf = new SparkConf().setAppName("XmlToJSON").setMaster("spark://127.0.0.1:7077")
            .set("es.index.auto.create", "true")
            .set("es.nodes", "127.0.0.1:9200")
            .set("es.write.operation","upsert")
            .set("es.mapping.id", "event_id")
            .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        
        val sc = new SparkContext(conf)
        
        val ssc = new StreamingContext(sc, Seconds(30))
        println("************************start stremaing *************************.")    
        val xmlDStream = ssc.textFileStream("hdfs://127.0.0.1:9000/user/cruise/streaming")
        println("************************ create DStream *************************.")

        
        xmlDStream.foreachRDD { xmlRdd =>
            // Get the singleton instance of SparkSession
            val spark = SparkSession.builder.config(xmlRdd.sparkContext.getConf).getOrCreate()
            import spark.implicits._
            println("let me check. ==  = = =   "  + xmlRdd.collect().length + "    ---------------")
            //val sc = new SparkContext(xmlRdd.sparkContext.getConf)
            //val sqlContext = new org.apache.spark.sql.SQLContext
            
            //val xmlDataFrame = xmlRdd.toDF(spark.sqlContext, xmlRdd)
            
            //xmlDataFrame.show()
            
            val fields = Seq("event_id", "network_name","_updateTime", "xmlns", "affected_lanes", "atis_severity", "category", "contact", "message_codes", "location","road_conditions", "secondaryLoc", "severity", "source", "start_timestamp", "status","type", "type_description", "update_timestamp", "update_type", "weather_conditions")
            
            if(xmlRdd.collect().length > 0){
                
                val rdd = ssc.sparkContext.parallelize(Seq(xmlRdd.collect().mkString("")))
                var df = new XmlReader().withRowTag("eventData").xmlRdd(spark.sqlContext, rdd)
                
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

                newdf.saveToEs("spark/events")
                
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