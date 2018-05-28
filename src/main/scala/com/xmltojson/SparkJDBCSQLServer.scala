package com.xmltojson

import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.immutable.List
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.TaskContext
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.rdd.EsSpark
import scala.collection.mutable.ArrayBuffer

//import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import org.apache.kafka.common.TopicPartition


object SparkJDBCSQLServer {
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
        
        
        val schemaString = "EVENT_RECORD_ID EVENT_ID NETWORK_ID EVENT_CATEGORY_ID EVENT_SEVERITY_ID UPDATE_TIME PRI_LOCATION_RECORD PRI_LOCATION_LATITUDE PRI_LOCATION_LONGITUDE PRI_LOCATION_ROAD PRI_LOCATION_DIRECTION PRI_LOCATION_OFFSET PRI_LOCATION_CROSS PRI_LOCATION_CITY PRI_LOCATION_COUNTY SEC_LOCATION_RECORD SEC_LOCATION_LATITUDE SEC_LOCATION_LONGITUDE SEC_LOCATION_ROAD SEC_LOCATION_DIRECTION SEC_LOCATION_OFFSET SEC_LOCATION_CROSS SEC_LOCATION_CITY SEC_LOCATION_COUNTY EVENT_STATUS_ID EVENT_DESC"

        val fields = schemaString.split(" ").map(fieldname => StructField(fieldname, StringType, nullable = true))
        
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "group.id" -> "test1",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        
        val topics = Array("events")
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        /*
        stream.foreachRDD { rdd =>
            // Get the singleton instance of SparkSession TestData2_original.parquet
            println("let me check. ==  = = =   "  + rdd.collect().length + "    ---------------")
            val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
            import spark.implicits._
            
            val schema = StructType(fields)
            val messages = rdd.map(r => r.value.toString).map (r => r.split(","))
        	    if(messages.collect().length > 0){
        	    	  
        	    	    val mapData = messages.map {r => 
        	    	      val array = ArrayBuffer[String]()
        	    	      for (i <- 0 until r.length) {
        	    	         array += r(i)
        	    	      }
        	    	      Row.fromSeq(array.toSeq)
        	    	  }
        	    	  
        	    	  val df = spark.createDataFrame(mapData, schema)
        	    	  
        	    	  df.printSchema()
        	    	  
        	    	  df.show(5)
        	    	  val newdf = df.select(
        	    	      $"NETWORK_ID".alias("network_id"),
        	    	      $"EVENT_ID".alias("event_id")
        	    	  )
        	    	  newdf.printSchema()
        	    	  newdf.show()
               newdf.saveToEs("spark/events")
                
            } else {
                println("~~~~~~~~~~~~~~~~~~No New data found~~~~~~~~~~~~~~~~~~~")
            }
        }
        
        println()
        println("------------------------ printing DStreamig -------------------------")
        stream.print()
        println(" --------------------printing DStreamig&&&&&&&&&&&")
        * 
        */
        ssc.start()
        ssc.awaitTermination()
        
        
    }
}