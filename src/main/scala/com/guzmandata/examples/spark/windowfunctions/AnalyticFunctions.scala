package com.guzmandata.examples.spark.windowfunctions

import com.guzmandata.examples.spark.commons.DataDummy.createEmployeeDummy
import com.guzmandata.examples.spark.commons.InitSpark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{cume_dist, desc, lag, lead}

object AnalyticFunctions extends App with InitSpark {

  /**
   * WindowSpec
   */
  val windowSpec = Window.partitionBy("department").orderBy(desc("salary"))

  val employeeDf = createEmployeeDummy()
  employeeDf.show()


  /**
   * cume_dist Window Function
   * La función de ventana cume_dist () se usa para obtener
   * la distribución acumulativa de valores dentro de una
   * partición de ventana
   */
  employeeDf.withColumn("cume_dist", cume_dist().over(windowSpec))
    .show()


  /**
   * lag Window Function
   */
  employeeDf.withColumn("lag", lag("salary",2).over(windowSpec))
    .show()

  /**
   *  lead Window Function
   */
  employeeDf.withColumn("lead",lead("salary",2).over(windowSpec))
    .show()
}
