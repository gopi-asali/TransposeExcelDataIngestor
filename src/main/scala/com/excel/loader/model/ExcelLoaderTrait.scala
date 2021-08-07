package com.excel.loader.model

import com.excel.loader.util.ContextHub
import org.apache.spark.sql.{DataFrame, SparkSession}

trait ExcelLoaderTrait{

  val spark: SparkSession = ContextHub.getSparkSession

  type transformedDataType

  def loadData(filePath: String): DataFrame

  def transform(inputDataSet:  DataFrame ):transformedDataType

  def writeData(writeDataSet: transformedDataType): Unit

  protected def readExcelFile(filePath: String): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")
      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .load(filePath)
  }
}
