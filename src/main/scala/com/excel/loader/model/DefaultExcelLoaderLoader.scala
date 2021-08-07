package com.excel.loader.model
import org.apache.spark.sql.DataFrame

object DefaultExcelLoaderLoader  extends  ExcelLoaderTrait  {

  override type transformedDataType = DataFrame

  override def loadData(filePath: String):transformedDataType = {
    readExcelFile(filePath)
  }

  override def transform(inputDataSet: transformedDataType): transformedDataType = inputDataSet

  override def writeData(writeDataSet: transformedDataType): Unit = writeDataSet.show()
}
