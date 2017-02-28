package org.custom.etl.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.serializer.KryoSerializer

object SparkConnector {
  
  val conf = new SparkConf().setMaster("local[4]").setAppName("CUSTOM-ETL-SPARK2ES")
//  conf.set("es.nodes", "localhost")
//  conf.set("es.port", "9200") 
//  conf.set("es.index.auto.create", "true")
  conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//  conf.set("es.nodes.wan.only", "true")
  val sc = new SparkContext(conf)
  
  val sqlContext = new SQLContext(sc)
  
}