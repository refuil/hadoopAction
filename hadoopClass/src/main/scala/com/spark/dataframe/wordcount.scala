package com.spark.dataframe

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object wordcount {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val session: SparkSession = SparkSession
      .builder().
      appName("SQLWordCount")
      .master("local[4]").getOrCreate()

    //读数据，是lazy
    //Dataset也是一个分布式数据集，是对RDD的进一步分装
    //Dataset只有一列，默认这列叫value
    val lines: Dataset[String] = session.read.textFile(args(0))

    //导入隐式转换
    import session.implicits._
    val word: Dataset[String] = lines.flatMap(_.split(","))

    //注册表
    word.createTempView("test")

    //执行SQL
    val result: DataFrame = session
      .sql("SELECT value,COUNT(*) counts FROM test GROUP BY value ORDER BY counts DESC")
    result.show()
    session.stop()
  }
}
