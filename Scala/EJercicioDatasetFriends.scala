package com.sundogsoftware.spark


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object EJercicioDatasetFriends {

  case class Person(id:Int, name:String,age:Int,friends:Int)

  def main(args:Array[String]){

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Friends")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val people = spark.read
      .option("header","true")
      .option("inferSchema","True")
      .csv("data/fakefriends.csv")
      .as[Person]


    val agesfriend = people.select("age","friends")

    agesfriend.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").show()



    spark.stop()
  }

}
