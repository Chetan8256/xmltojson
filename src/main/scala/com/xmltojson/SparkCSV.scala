package com.xmltojson

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.rdd.EsSpark

object SparkCSV {
    def main (args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("XmlToJSON").setMaster("spark://127.0.0.1:7077")
            .set("es.index.auto.create", "true")
            .set("es.nodes", "127.0.0.1:9200")
            .set("es.write.operation","upsert")
            .set("es.mapping.id", "event_id")
            
        val spark = SparkSession
              .builder()
              .appName("XmlToJson")
              .getOrCreate()
              
         import spark.implicits._  
         val df = spark.read.format("csv").option("header", "true").load("csvfilepath")
         
         df.saveToEs("spark/data")
    }
       
}