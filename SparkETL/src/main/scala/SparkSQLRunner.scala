package com.rh.bi.etl.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.hive._
import com.databricks.spark.avro._
import org.apache.hadoop.fs._
import scala.io.Source._
import sys.process._

object SparkSQLRunner {

  def main(args: Array[String]) {

    val sqlScript = args(0)
    val batchId = args(1)
    val avroOutputPath = args(2)

    val spark = SparkSession
      .builder()
      .appName("SparkSQLApp")
     .master("local[*]")
      .config("hive.metastore.uris", "thrift://sdbidinhdpl002.reyesholdings.com:9083")
      .enableHiveSupport()
      .getOrCreate();

    val executeSQL = fromFile(s"$sqlScript").getLines.mkString
    spark.sql(s"$executeSQL").write.mode(SaveMode.Overwrite).avro(avroOutputPath)
    spark.stop();
  }
}

