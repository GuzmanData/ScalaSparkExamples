package com.guzmandata.examples.spark.windowfunctions

import com.guzmandata.examples.spark.commons.DataDummy.createEmployeeDummy
import com.guzmandata.examples.spark.commons.InitSpark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{dense_rank, desc, ntile, percent_rank, rank, row_number}

object RankingFunctions extends App with InitSpark {
  val employeeDf = createEmployeeDummy()
  employeeDf.show()

  /**
   * WindowSpec
   */
  val windowSpec = Window.partitionBy("department").orderBy(desc("salary"))


  /**
   * row_number Window Function
   * La función de ventana se usa para dar el número
   * de fila secuencial comenzando desde 1 hasta el
   * resultado de cada partición de ventana.
   */
  employeeDf.withColumn("row_number", row_number.over(windowSpec)).show()

  /**
   * rank Window Function
   * La función de ventana rank () se utiliza para proporcionar un
   * rango al resultado dentro de una partición de ventana.
   * Esta función deja huecos de rango cuando hay empates.
   */
  employeeDf.withColumn("rank", rank().over(windowSpec))
    .show()


  /**
   * dense_rank Window Function
   * La función de ventana dense_rank () se usa para obtener
   * el resultado con el rango de filas dentro de una partición
   * de ventana sin espacios. Esto es similar a la diferencia de
   * la función de rango (), ya que la función de rango deja
   * espacios en el rango cuando hay empates.
   */
  employeeDf.withColumn("dense_rank", dense_rank().over(windowSpec))
    .show()

  /**
   * percent_rank Window Function
   */
  employeeDf.withColumn("percent_rank", percent_rank().over(windowSpec))
    .show()

  /**
   * ntile Window Function
   * La función de ventana ntile () devuelve el rango relativo de las
   * filas de resultados dentro de una partición de ventana. En el
   * siguiente ejemplo, hemos utilizado 2 como argumento para ntile,
   * por lo que devuelve una clasificación entre 2 valores (1 y 2)
   */
  employeeDf.withColumn("ntile",ntile(2).over(windowSpec))
    .show()
}
