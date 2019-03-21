package com.rh.bi.etl.spark

import java.sql.{Connection, DriverManager}

import scala.collection.mutable.ArrayBuffer
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File

import com.rh.bi.etl.spark.QueryBuilder.columnListExtended

import sys.process._


object jdbcConnector {

  val endpoints_conf: Config = ConfigFactory.parseFile(new File("/usr/hdf/current/nifi/conf/nifi.reyes.bi.endpoints.properties"))
  val drivers_conf: Config = ConfigFactory.parseFile(new File("/usr/hdf/current/nifi/conf/nifi.reyes.bi.drivers.properties"))

  //val drivers_conf: Config = ConfigFactory.parseFile(new File("/Users/theena/Documents/Metadata/nifi-conf/dev/nifi.reyes.bi.drivers.properties"))
 // val endpoints_conf: Config = ConfigFactory.parseFile(new File("/Users/theena/Documents/Metadata/nifi-conf/dev/nifi.reyes.bi.endpoints.properties"))

  val jdbcUrl = endpoints_conf.getString("com.reyes.bi.db.connection.url.sqldw.datamart")
  val jdbcUsername = endpoints_conf.getString("com.reyes.bi.db.connection.user.sqldw.datamart")

  // val decryptCmd: String = s"""/datalake/metadata/nifi-conf/scripts/decrypt.sh /usr/hdf/current/nifi/conf/com.reyes.bi.db.connection.password.sqldw.datamart.enc""".stripMargin
  //val jdbcPassword_tmp = decryptCmd!!
  //val jdbcPassword=jdbcPassword_tmp.stripLineEnd
  // println(jdbcPassword + "Hi")

  val jdbcPassword = endpoints_conf.getString("com.reyes.bi.db.connection.password.sqldw.datamart")
  val driverClass = drivers_conf.getString("com.reyes.bi.db.connection.driverclass.sqldw")


  def getDBConnection(): Connection = {

    var connection: Connection = null
    var count: Integer = 0

    while (connection == null && count<3){
      try {

        Class.forName(driverClass);
        connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

      }
      catch {
        case e: Exception => System.err.println("Get DB Connection failed, retrying...: " + count + "\n" + e);
          count = count + 1 ;
          Thread.sleep(3000)
      }
    }
    return connection;
  }



    def getTableColumn(jdbcDbSchemaOwner: String, targetTable: String): ArrayBuffer[String] = {


    var tgtColumns = ArrayBuffer[String]()
    var tgtColumnsStructure: Map[String, String] = Map()
    var connection: Connection = null
    try {
      // make the connection
      //Class.forName(driverClass)
     // connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
     connection=getDBConnection()
      val dbmd = connection.getMetaData
      val r = dbmd.getColumns(null, jdbcDbSchemaOwner, targetTable, null)
      val rm = r.getMetaData
      val columnCount = rm.getColumnCount
      var columnName: String = null
      var columnDataType: String = null

      while (r.next) {
        columnName = r.getString("COLUMN_NAME")
        columnDataType = r.getString("TYPE_NAME") + "(" + r.getString("COLUMN_SIZE") + ")"
        tgtColumns += r.getString("COLUMN_NAME")
        // tgtColumnsStructure += (columnName  -> columnDataType )
      }
    } catch {
      case e: Throwable => e.printStackTrace
        connection.close()

    }
    connection.close()

    tgtColumns
  }

  def getTableColumnTypesExtended(jdbcDbSchemaOwner: String, targetTable: String): Map[String, String] = {


    var tgtColumns = ArrayBuffer[String]()
    var tgtColumnTypesExtended: Map[String, String] = Map()
    var connection: Connection = null
    try {
      // make the connection
      //Class.forName(driverClass)
      // connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
      connection=getDBConnection()
      val dbmd = connection.getMetaData
      val r = dbmd.getColumns(null, jdbcDbSchemaOwner, targetTable, null)
      val rm = r.getMetaData
      val columnCount = rm.getColumnCount
      var columnName: String = null
      var columnDataType: String = null

      while (r.next) {
        columnName = r.getString("COLUMN_NAME")
       // println(r.getString("COLUMN_NAME")+ " "+r.getString("TYPE_NAME")  + " "+ r.getString("COLUMN_SIZE") + ","+ r.getString("DECIMAL_DIGITS"))
      if (r.getString("TYPE_NAME").equalsIgnoreCase("numeric")) {
          columnDataType = r.getString("TYPE_NAME") + "(" + r.getString("COLUMN_SIZE")+ ","+ r.getString("DECIMAL_DIGITS") + ")"
          }
      else {
  columnDataType = r.getString("TYPE_NAME") + "(" + r.getString("COLUMN_SIZE") + ")"
    }


        tgtColumns += r.getString("COLUMN_NAME")
        tgtColumnTypesExtended += (columnName -> columnDataType)
      }
    } catch {
      case e: Throwable => e.printStackTrace
        connection.close()

    }
    connection.close()

    tgtColumnTypesExtended
  }


  def getTableColumnTypes(jdbcDbSchemaOwner: String, targetTable: String): Map[String, String] = {

    var tgtColumns = ArrayBuffer[String]()
    var tgtColumnTypes: Map[String, String] = Map()
    var connection: Connection = null
    try {
      // make the connection
      //Class.forName(driverClass)
      // connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
      connection=getDBConnection()
      val dbmd = connection.getMetaData
      val r = dbmd.getColumns(null, jdbcDbSchemaOwner, targetTable, null)
      val rm = r.getMetaData
      val columnCount = rm.getColumnCount
      var columnName: String = null
      var columnDataType: String = null

      while (r.next) {
        columnName = r.getString("COLUMN_NAME")
        columnDataType = r.getString("TYPE_NAME")

        tgtColumns += r.getString("COLUMN_NAME")
        tgtColumnTypes += (columnName -> columnDataType)
      }
    } catch {
      case e: Throwable => e.printStackTrace
        connection.close()

    }
    connection.close()

    tgtColumnTypes
  }


  def getTablePrimaryKeys(jdbcDbSchemaOwner: String, targetTable: String): ArrayBuffer[String] = {

    var tgtColumns = ArrayBuffer[String]()
    var tgtColumnsStructure: Map[String, String] = Map()
    var connection: Connection = null
    try {
      // make the connection
      //Class.forName(driverClass)
      // connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
      connection=getDBConnection()
      val dbmd = connection.getMetaData
      val r = dbmd.getPrimaryKeys(null, jdbcDbSchemaOwner, targetTable)
      val rm = r.getMetaData
      val columnCount = rm.getColumnCount
      var columnName: String = null
      var columnDataType: String = null

      while (r.next) {
        tgtColumns += r.getString("COLUMN_NAME")
        // tgtColumnsStructure += (columnName  -> columnDataType )
      }
    } catch {
      case e: Throwable => e.printStackTrace
        connection.close()

    }
    connection.close()
    tgtColumns
  }

}