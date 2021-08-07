package com.excel.loader

import java.io.File

import com.excel.loader.model.{DefaultExcelLoaderLoader, ExcelLoaderTrait, MultiTableExcelLoader, TransposeExcelLoaderLoader}

object ExcelMain {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      throw new IllegalArgumentException(
        "File name should be provided in the first parameter"
      )
    }
    val filePaths = args(0)
      .split(",")
      .map(
        filename => new File(s"src/main/resources/$filename").getAbsolutePath
      )
    val loaderIns = loaderInitialize(args)
    filePaths.par
      .map(loaderIns.loadData)
      .map(value => loaderIns.transform(value))
      .foreach(loaderIns.writeData)

  }

  def loaderInitialize(args: Array[String]): ExcelLoaderTrait = {

    args.length match {
      case 1 => DefaultExcelLoaderLoader
      case _ if args(1).toUpperCase.contains("TRANSPOSE") =>
        TransposeExcelLoaderLoader
      case _ if args(1).toUpperCase.contains("MULTITABLE") =>
        MultiTableExcelLoader
    }

  }

}
