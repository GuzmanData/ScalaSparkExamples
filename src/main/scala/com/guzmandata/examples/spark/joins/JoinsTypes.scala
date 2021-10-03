package com.guzmandata.examples.spark.joins

import com.guzmandata.examples.spark.commons.{InitSpark}
import com.guzmandata.examples.spark.commons.DataDummy.{createAddressDummyDf, createCustomerDummyDf}

object JoinsTypes extends App with InitSpark {

  val customerDf = createCustomerDummyDf
  val addresDf = createAddressDummyDf

  /**
   * Ejemplo de left Join
   */
  customerDf.join(
    addresDf,
    Seq("customerID"),
    "left"
  ).show()

  /**
   * Ejemplo de inner Join
   */
  customerDf.join(
    addresDf,
    Seq("customerID"),
    "inner"
  ).show()


  /**
   * Ejemplo de cross Join
   */
  customerDf.crossJoin(addresDf).show()


}
