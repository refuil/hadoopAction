package com.hadoop.spark

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    //loading a csv
    val statDF = spark.read.option("header",
      "true").option("inferschema", "true").option("sep",
      ",").csv("statesPopulation.csv")

    val statesTaxRatesDF = spark.read.option("header",
      "true").option("inferschema", "true").option("sep",
      ",").csv("statesTaxRates.csv")


    //saving datasets
    statDF.write.option("header",
        "true").csv("statesPopulation_dup.csv")

    statesTaxRatesDF.write.option("header",
      "true").csv("statesTaxRates_dup.csv")

    statDF.printSchema()
  }
}
