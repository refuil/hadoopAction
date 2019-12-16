package com.hadoop.spark

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object FileOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    //
    //loading a csv
    val statesDF = spark.read.option("header", "true")
      .option("inferschema", "true")
      .option("sep", ",")
      .csv("statesPopulation.csv")

    val statesTaxRatesDF = spark.read.option("header", "true")
      .option("inferschema", "true")
      .option("sep", ",")
      .csv("statesTaxRates.csv")


    //saving datasets
    statesDF.write.option("header",
        "true").csv("statesPopulation_dup.csv")

    statesTaxRatesDF.write.option("header",
      "true").csv("statesTaxRates_dup.csv")

    statesDF.printSchema()
  }

  class FileOperation(spark: SparkSession){
    import spark.implicits._


  }


}
