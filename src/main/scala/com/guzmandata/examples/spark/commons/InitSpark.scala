package com.guzmandata.examples.spark.commons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait InitSpark {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Use new SparkSession interface in Spark 2.0
  val spark = SparkSession
    .builder
    .appName("SparkSQL")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  var sparkConf = new SparkConf().set("spark.sql.crossJoin.enabled", "true")


}
