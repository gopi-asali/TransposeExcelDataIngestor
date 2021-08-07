package com.excel.loader.model
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
object TransposeExcelLoaderLoader extends ExcelLoaderTrait {

  override type transformedDataType = DataFrame

  override def transform(df: DataFrame): DataFrame = {

    val groupColumns: Array[String] = "group_code,table_name".split(",")
//      spark.conf.get("spark.groupColumns").split(",")
    val outputColumns: Array[String] = "month,values".split(",")
//      spark.conf.get("spark.outputColumns").split(",")


    val transposeInitialColumns= df.columns.filterNot(groupColumns.contains)
    val transposeColumns = df.columns.filterNot(groupColumns.contains).flatMap(value => Seq(s"'$value'",value))


    println("Count of transpose columns : " + transposeInitialColumns.length)
    val selectColumns: Array[Column] = groupColumns
      .map(col)
      .+:(expr(s"stack(${transposeInitialColumns.length},${transposeColumns
        .mkString(",")}) as (${outputColumns.mkString(",")})"))

    val transformedDataFrame =
      df.select(selectColumns: _*)

    transformedDataFrame.show()
    transformedDataFrame
  }

  override def loadData(filePath: String): DataFrame = readExcelFile(filePath)

  override def writeData(df: DataFrame): Unit = df.show()
}
