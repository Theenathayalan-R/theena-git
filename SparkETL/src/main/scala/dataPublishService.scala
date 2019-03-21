package com.rh.bi.etl.spark

import java.io.File
import com.rh.bi.etl.spark.QueryBuilder.{columnListExtended, parseWithJackson}

import scala.collection.mutable.ListBuffer
import scala.io._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.hive._
import com.databricks.spark.avro._
import org.apache.hadoop.fs._
import java.sql.{Connection, DriverManager}
import sys.process._
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File

import com.rh.bi.etl.spark.jdbcConnector.{jdbcPassword, jdbcUsername}

object dataPublishService {

 // val endpoints_conf: Config = ConfigFactory.parseFile(new File("/usr/hdf/current/nifi/conf/nifi.reyes.bi.endpoints.properties"))
  //val drivers_conf: Config = ConfigFactory.parseFile(new File("/usr/hdf/current/nifi/conf/nifi.reyes.bi.drivers.properties"))

  //val drivers_conf: Config = ConfigFactory.parseFile(new File("/Users/theena/Documents/Metadata/nifi-conf/dev/nifi.reyes.bi.drivers.properties"))
  //val endpoints_conf: Config = ConfigFactory.parseFile(new File("/Users/theena/Documents/Metadata/nifi-conf/dev/nifi.reyes.bi.endpoints.properties"))

 // val jdbcUrl =  endpoints_conf.getString("com.reyes.bi.db.connection.url.sqldw.datamart")
 // val jdbcUsername = endpoints_conf.getString("com.reyes.bi.db.connection.user.sqldw.datamart")

 // val decryptCmd: String = s"""/datalake/metadata/nifi-conf/scripts/decrypt.sh /usr/hdf/current/nifi/conf/com.reyes.bi.db.connection.password.sqldw.datamart.enc""".stripMargin
  //val jdbcPassword_tmp = decryptCmd!!
  //val jdbcPassword=jdbcPassword_tmp.stripLineEnd
 // println(jdbcPassword + "Hi")

 // val jdbcPassword = endpoints_conf.getString("com.reyes.bi.db.connection.password.sqldw.datamart")
 // val driverClass = drivers_conf.getString("com.reyes.bi.db.connection.driverclass.sqldw")

  def main(args: Array[String]): Unit = {
    val targetTable = args(0)
    val metadataFilename = args(1)
    val avroInputPath = args(2)
    val batchId = args(3)
    val jdbcDbSchemaOwner = args(4)
    val libPath = args(5)
    var storedProcSchemaOwner = args(6)
    val etlBatchEpoch = args(7)

    var storedProc: String = null
    println(s"Reading JSON ${metadataFilename} ...")
    val jsonMetadataConfig = parseWithJackson(Source.fromFile(metadataFilename))

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("dataPublishService")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate();

    spark.sparkContext.setLogLevel("ERROR")

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", jdbcConnector.jdbcUsername)
    connectionProperties.put("password", jdbcConnector.jdbcPassword)
    connectionProperties.setProperty("Driver", jdbcConnector.driverClass)
    connectionProperties.setProperty("AutoCommit", "true")

    val fqTablename = jdbcDbSchemaOwner + "." + targetTable
    val fqTmpTablename = "["+storedProcSchemaOwner +"]"+ ".TMP_" + targetTable + "_" + batchId
    val TmpTablename = "TMP_" + targetTable + "_" + batchId

    val tgtColumns = jdbcConnector.getTableColumn(jdbcDbSchemaOwner, targetTable)
    val tgtColumnTypes = jdbcConnector.getTableColumnTypes(jdbcDbSchemaOwner, targetTable)
    val tgtColumnTypesExtended = jdbcConnector.getTableColumnTypesExtended(jdbcDbSchemaOwner, targetTable)

    val castColumns = generateSelectWithCastQuery(tgtColumns.toList, tgtColumnTypes, tgtColumnTypesExtended)

    val UpdateCondition = generateUpdateConditionForMerge(tgtColumns.toList, tgtColumnTypes, tgtColumnTypesExtended)
    val InsertSourceColumns = generateInsertSourceColumnsForMerge(tgtColumns.toList, tgtColumnTypes, tgtColumnTypesExtended)
    val InsertTargetColumns = generateInsertTargetColumnsForMerge(tgtColumns.toList)


    val df = spark.read.avro(avroInputPath)
    df.write.mode(SaveMode.Overwrite).jdbc(jdbcConnector.jdbcUrl, fqTmpTablename, connectionProperties)
    //var conn = DriverManager.getConnection(jdbcUrl, connectionProperties)
    var conn = jdbcConnector.getDBConnection()

    if (conn==null)     {
      System.err.println("Could not get DB connection");
      sys.exit(1)
    }


    if ((jsonMetadataConfig.logicalEntity == "DIM") && (jsonMetadataConfig.scdType == "Type1")) {
      storedProc =
        s"""EXEC ${storedProcSchemaOwner}.LOAD_DIM_DATA_TYPE1_UPSERT '${fqTmpTablename}','${fqTablename}','${jsonMetadataConfig.sidKey}','${castColumns}','${UpdateCondition}','${InsertSourceColumns}','${InsertTargetColumns}' """.stripMargin
    }
    else if ((jsonMetadataConfig.logicalEntity == "DIM") && (jsonMetadataConfig.scdType == "Type2")) {
      storedProc =
        s"""EXEC ${storedProcSchemaOwner}.LOAD_DIM_DATA_TYPE2_UPSERT '${fqTmpTablename}','${fqTablename}','${jsonMetadataConfig.sidKey}','${castColumns}','${UpdateCondition}','${InsertSourceColumns}','${InsertTargetColumns}' """.stripMargin

    }
    else {
      storedProc =
        s"""EXEC ${storedProcSchemaOwner}.LOAD_FACT_DATA_INSERT '${storedProcSchemaOwner}','${TmpTablename}','${jdbcDbSchemaOwner}','${targetTable}','${etlBatchEpoch}','${castColumns}' """.stripMargin

    }
    System.out.println("Running Stored Procedure : \n" + storedProc);
    var exitCode = 1

    try {
      val rs = conn.createStatement.executeQuery(storedProc)
      while(rs.next()) {
        if (rs.getString("STATUS") == "SUCCEEDED") {
          exitCode = 0
          System.err.println("STATUS:" + rs.getString("STATUS") + "\n Exit code:" + exitCode);
        }
        else {
          exitCode = 1
        }
      }
    }
    catch
      {
        case e: Exception => System.err.println("Exception caught, exit code: " + exitCode + "\n" + e);

      }

    finally
    {
      conn.close()
      sys.exit(exitCode)
    }
    sys.exit(exitCode)


  }


  def generateSelectWithCastQuery(tgtColumns: List[String],tgtColumnTypes : Map[String, String],tgtColumnTypesExtended : Map[String, String]): String = {
    var columnsWithCast : ListBuffer[String]= ListBuffer()
    var columntype:String = null
    var columntypeExtended:String = null

    for (columns <- tgtColumns) {
      columntype = tgtColumnTypes.get(columns).mkString
      columntypeExtended = tgtColumnTypesExtended.get(columns).mkString
      if (columnListExtended.contains(columntype)) {
        columnsWithCast += ("CAST(" + columns + " AS " + columntypeExtended + ") AS " + columns)
      }
      else   {    columnsWithCast += ("CAST(" + columns + " AS " + columntype + ") AS " + columns)
      }
      // columnsWithCast += ("CAST(" + columns + " AS " + columntype + ") AS " + columns)
    }
    val selectWithCast: String =
      s""" ${columnsWithCast.mkString(",")}""".stripMargin
    selectWithCast

  }


  def generateUpdateConditionForMerge(tgtColumns: List[String],tgtColumnTypes : Map[String, String],tgtColumnTypesExtended : Map[String, String]): String = {
    var updateConditions : ListBuffer[String]= ListBuffer()
    var columntype:String = null
    var columntypeExtended:String = null

    for (columns <- tgtColumns) {
      columntype = tgtColumnTypes.get(columns).mkString
      columntypeExtended = tgtColumnTypesExtended.get(columns).mkString

      if (columnListExtended.contains(columntype)) {
        updateConditions += ("TARGET."+columns+" = CAST(SOURCE."+columns + " AS " + columntypeExtended + ")")
      }
      else   {    updateConditions += ("TARGET."+columns+" = CAST(SOURCE." + columns + " AS " + columntype + ")")
      }
      // columnsWithCast += ("CAST(" + columns + " AS " + columntype + ") AS " + columns)

    }
    val selectWithCast: String =
      s""" ${updateConditions.mkString(",")}""".stripMargin
    selectWithCast

  }

  def generateInsertSourceColumnsForMerge(tgtColumns: List[String],tgtColumnTypes : Map[String, String],tgtColumnTypesExtended : Map[String, String]): String = {
    var insertSourceColumns : ListBuffer[String]= ListBuffer()
    var columntype:String = null
    var columntypeExtended:String = null

    for (columns <- tgtColumns) {
      columntype = tgtColumnTypes.get(columns).mkString
      columntypeExtended = tgtColumnTypesExtended.get(columns).mkString

      if (columnListExtended.contains(columntype)) {
        insertSourceColumns += ("CAST(SOURCE."+columns + " AS " + columntypeExtended + ")")
      }
      else   {    insertSourceColumns += ("CAST(SOURCE." + columns + " AS " + columntype + ")")
      }
      // columnsWithCast += ("CAST(" + columns + " AS " + columntype + ") AS " + columns)

    }
    val selectWithCast: String =
      s""" ${insertSourceColumns.mkString(",")}""".stripMargin
    selectWithCast

  }


  def generateInsertTargetColumnsForMerge(tgtColumns: List[String]): String = {
    var insertTargetColumns : ListBuffer[String]= ListBuffer()
    var columntype:String = null
    var columntypeExtended:String = null

    for (columns <- tgtColumns) {

   insertTargetColumns += columns

    }
    val selectWithCast: String =
      s""" ${insertTargetColumns.mkString(",")}""".stripMargin
    selectWithCast

  }
}