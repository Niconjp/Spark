package com.sundogsoftware.spark


import org.apache.log4j._
import org.apache.spark.sql.functions.{round,sum}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType,StructType,DoubleType}




object EjerciciototalSpectdataset {

  case class Customer(id:Int,item:Int,price:Double)

  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("totalSpent")
      .master("local[*]")
      .getOrCreate()

    val CustomerSchema = new StructType()
      .add("id",IntegerType,nullable = true)
      .add("item",IntegerType,nullable=true)
      .add("price",DoubleType,nullable=true)

    import spark.implicits._
    val ds = spark.read
      .schema(CustomerSchema)
      .csv{"data/customer-orders.csv"}
      .as[Customer]

    val totalSpent =ds
      .groupBy("id")
      .agg(round(sum("price"),2)
      .alias("total_spent"))

    val results= totalSpent.sort("total_spent")

    results.show(totalSpent.count.toInt)


    spark.stop()
  }

}
