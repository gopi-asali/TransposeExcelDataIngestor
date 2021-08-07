<h1>TransposeExcelDataIngestor</h1>
A library for querying Excel files with Apache Spark and perform below transformations .

### Three type of transformations.

1. Ingest as it is into the target
2. Unpivot the table
3. Pick up multiple tables from single sheet and convert them as various dataFrames . 

### Requirements
This library requires Spark 2.0+

### Using with Spark shell
This package require com.crealytics:spark-excel that can be used in --packages command line option. For example, to include it when starting the spark shell:

Spark compiled with Scala 2.12
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.12:0.13.1
Spark compiled with Scala 2.11
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.11:0.13.1

### Features
This package allows querying Excel spreadsheets as Spark DataFrames.
