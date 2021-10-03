package com.guzmandata.examples.spark.windowfunctions

import com.guzmandata.examples.spark.commons.DataDummy.createEmployeeDummy
import com.guzmandata.examples.spark.commons.InitSpark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, desc, max, min, row_number, sum}

object AggregateFunctions extends App with InitSpark {

  /**
   * WindowSpec
   */
  val windowSpecAgg = Window.partitionBy("department")
  val employeeDf = createEmployeeDummy()
  employeeDf.show()

  employeeDf.withColumn("row", row_number.over(windowSpecAgg.orderBy(desc("salary"))))
    .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
    .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
    .withColumn("min", min(col("salary")).over(windowSpecAgg))
    .withColumn("max", max(col("salary")).over(windowSpecAgg))
    .where(col("row") === 1).select("department", "avg", "sum", "min", "max")
    .show()


}
