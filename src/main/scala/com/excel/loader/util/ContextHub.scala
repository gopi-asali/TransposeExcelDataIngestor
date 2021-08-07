package com.excel.loader.util

import org.apache.spark.sql.SparkSession

object ContextHub {

  private var spark: SparkSession = _

  def getSparkSession: SparkSession = {

    if (spark == null)
      this.spark = SparkSession
        .builder()
        .master("local")
        .appName("ExcelParser")
        .getOrCreate()

    spark
  }

  def setSparkSession(spark: SparkSession): SparkSession = {
    this.spark = spark
    spark
  }

}
