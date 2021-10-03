package com.guzmandata.examples.spark.commons

import org.apache.spark.sql.DataFrame

object DataDummy extends InitSpark {
  case class Customer(customerID: Int, name: String = "", DateOfBrith: String = "", Gender: String = "")

  case class Address(customerId: Int, address: String = "")

  /**
   *
   * @return DataFrame dummy con información del cliente: customer_id, name, date of birth, gender
   */
  def createCustomerDummyDf(): DataFrame = {
    import spark.implicits._
    val customerList = List(
      Customer(1, "Carlos", "1989-16-08", "masculino"),
      Customer(2, "Andres", "1990-11-11", "masculino"),
      Customer(3, "Saymon", "1987-14-02", "masculino"),
      Customer(4, "Grediana", "1994-07-01", "femenino"),
      Customer(5, "Lucia", "2001-07-12", "femenino")
    )
    spark.sparkContext.parallelize(customerList).toDF()
  }

  /**
   *
   * @return Dataframe dummy con la información de la direcciones de los clientes
   */
  def createAddressDummyDf(): DataFrame = {
    import spark.implicits._
    val addressList = List(
      Address(1, "Calle perseo 245"),
      Address(2, "Calle faisanes 247"),
      Address(2, "Calle margarita 178"),
      Address(10, "Calle margarita 178"),
      Address(3, "Calle lucero 47")
    )
    spark.sparkContext.parallelize(addressList).toDF()
  }

  def createEmployeeDummy(): DataFrame = {
    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    simpleData.toDF("employee_name", "department", "salary")

  }





}
