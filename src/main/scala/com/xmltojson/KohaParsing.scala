package com.xmltojson

import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf }
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, Map}


object KohaParsing {
	def main( args : Array [ String ] ) : Unit = {

		val conf = new SparkConf().setAppName("XmlToJSON").setMaster("spark://127.0.0.1:7077")
		val spark = SparkSession
		    .builder()
		    .appName("XmlToJson")
  		    .config(conf)
		    .getOrCreate()


		// For implicit conversions like convertinTopicPartitiong RDDs to DataFrames
		import spark.implicits._
		var df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").load("/Users/cruise/Downloads/detail.csv")

		df.show();
		var arr = ArrayBuffer[String]()
		val process: RDD[Map[String, String]] = df.rdd.map { row =>
			var subjectmap = Map[String, String]()
			if(row(0) != null && row(0).toString.contains("Accn Nos")) {
				subjectmap(row(1).toString) = arr.mkString("~~")
				arr = ArrayBuffer[String]()
			} else {
				//val pattern =
				if(row(1) != null && ! """1\.|2\.|3\.|4\.|5\.|6\.|7\.|8\.|9\.|10\.|11\.|12\.|I\.|II\.|III\.|IV\.|V\.|VI\.|VII\.""".r.findAllMatchIn(row(1).toString).isEmpty) {
					arr += row(1).toString
				}
			}
			if (subjectmap.keys.toArray.length > 0){
				subjectmap
			} else {
				subjectmap
			}
		}.filter(r => (r.keys.toArray.length > 0))

		process.take(50).foreach( r => println(r))

		val schema = StructType(Array("Accessno", "Subjects").map(fieldname => StructField(fieldname, StringType, nullable =
		  true)))

		val finaldata = process.map { r =>
		    val key = r.keys.toArray
			Row(key(0), r(key(0)))
		}

		val dataframe = spark.createDataFrame(finaldata, schema)
		//finaldata.toDF().createOrReplaceTempView("kohadata")

		//val dataframe = spark.sql("select * from kohadata")

		dataframe.show()
		//.coalesce(1).
		dataframe.write.format("com.databricks.spark.csv").option("delimiter", "^").mode("overwrite").save("/Users/cruise/Downloads/output.csv")

	}
}

