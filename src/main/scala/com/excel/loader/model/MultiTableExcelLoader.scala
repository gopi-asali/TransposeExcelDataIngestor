package com.excel.loader.model

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable

object MultiTableExcelLoader extends ExcelLoaderTrait {

  override type transformedDataType = Seq[DataFrame]

  override def loadData(filePath: String): DataFrame = readExcelFile(filePath)

  override def transform(inputDataSet: DataFrame): Seq[DataFrame] = {

    inputDataSet.show()

    val sourceTableSchema: StructType = inputDataSet.schema

    val tableNameColumn: String = "TableNames"
    val listOfTables = "Table1,Table2".split(",").toList
//      spark.conf.get("spark.outputColumns").split(",")

    val initialValue = (listOfTables, List.empty[(String, Row)])

    val aggregatedDf: immutable.Seq[(String, Row)] = inputDataSet.rdd
      .aggregate(initialValue)(
        (tableList: (List[String], List[(String, Row)]), row: Row) => {

          val secondEle: List[(String, Row)] = tableList._2
          val rowHeder: String = row.getAs[String](tableNameColumn)
          if (tableList._1.nonEmpty && !tableList._2
                .map(_._1)
                .contains(tableList._1.head) && tableList._1.head == rowHeder) {
            val tableName = tableList._1.head
            val newMap = secondEle ++ List(Tuple2(tableName, row))
            val UpdatedList = tableList._1.filterNot(_ == tableName)
            (UpdatedList, newMap)
          } else {
            val newMap = secondEle ++ List(Tuple2(tableList._2.last._1, row))
            (tableList._1, newMap)
          }

        },
        (table1: (List[String], List[(String, Row)]),
         table2: (List[String], List[(String, Row)])) => {
          val firstEle: List[(String, Row)] = table1._2
          val secondEle: List[(String, Row)] = table2._2

          (table1._1 ++ table2._1, firstEle ++ secondEle)
        }
      )
      ._2

    val filteredDf: immutable.Seq[(String, Row)] = aggregatedDf.filterNot(
      tup => tup._1 == tup._2.getAs[String](tableNameColumn)
    )

    import scala.collection.JavaConversions._
    filteredDf
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .mapValues(
        (rowSet: immutable.Seq[Row]) =>{
          spark.createDataFrame(rowSet, sourceTableSchema)
        }
      ).values.toSeq

  }

  override def writeData(writeDataSet: Seq[DataFrame]): Unit = writeDataSet.foreach(_.show())
}
