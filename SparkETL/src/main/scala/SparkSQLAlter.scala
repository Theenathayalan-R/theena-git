package com.rh.bi.etl.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.hive._
import com.databricks.spark.avro._

import scala.io.Source._

object SparkSQLAlter {

  def main(args: Array[String]) {

    val tableName = args(0)
    val partitionName = args(1)
    val location = args(2)


    val spark = SparkSession
      .builder()
      .appName("SparkSQLAlter")
      .master("local[*]")
    //  .config("spark.driver.extraJavaOptions", "-Dderby.system.home=file:///C:/tmp/spark_warehouse/derby")
   // .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark_warehouse/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate();
    println(spark.conf.getAll.mkString("\n"))
 //sql("CREATE TABLE IF NOT EXISTS src3 (key INT, value STRING)")
    val alterSQL="ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s ) LOCATION  %s".format(tableName, partitionName, location);
    println(alterSQL);

      spark.sql("show tables").show()
   // val executeSQL = fromFile(s"$sqlScript").getLines.mkString
    spark.sql(s"$alterSQL")
    spark.stop();
  }
}

