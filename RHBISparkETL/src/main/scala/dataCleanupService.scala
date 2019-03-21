package com.rh.bi.etl.spark

import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
import java.sql.DriverManager

import com.rh.bi.etl.spark.QueryBuilder.{columnListExtended, parseWithJackson}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io._

object dataCleanupService {

  val endpoints_conf: Config = ConfigFactory.parseFile(new File("/usr/hdf/current/nifi/conf/nifi.reyes.bi.endpoints.properties"))
  //val endpoints_conf: Config = ConfigFactory.parseFile(new File("/Users/theena/Documents/Metadata/nifi-conf/dev/nifi.reyes.bi.endpoints.properties"))
  val jdbcUrl =  endpoints_conf.getString("com.reyes.bi.db.connection.url.sqldw.datamart")
  val jdbcUsername = endpoints_conf.getString("com.reyes.bi.db.connection.user.sqldw.datamart")

 // val decryptCmd: String = s"""/datalake/metadata/nifi-conf/scripts/decrypt.sh /usr/hdf/current/nifi/conf/com.reyes.bi.db.connection.password.sqldw.datamart.enc""".stripMargin
  //val jdbcPassword_tmp = decryptCmd!!
  //val jdbcPassword=jdbcPassword_tmp.stripLineEnd
 // println(jdbcPassword + "Hi")

  val jdbcPassword = endpoints_conf.getString("com.reyes.bi.db.connection.password.sqldw.datamart")
  val drivers_conf: Config = ConfigFactory.parseFile(new File("/usr/hdf/current/nifi/conf/nifi.reyes.bi.drivers.properties"))
  //val drivers_conf: Config = ConfigFactory.parseFile(new File("/Users/theena/Documents/Metadata/nifi-conf/dev/nifi.reyes.bi.drivers.properties"))
  val driverClass = drivers_conf.getString("com.reyes.bi.db.connection.driverclass.sqldw")

  def main(args: Array[String]): Unit = {
    val targetTable = args(0)
    val jdbcDbSchemaOwner = args(1)
    var storedProcSchemaOwner = args(2)


    var storedProc: String = null
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    connectionProperties.setProperty("Driver", driverClass)
    connectionProperties.setProperty("AutoCommit", "true")

    val fqTablename = jdbcDbSchemaOwner + "." + targetTable
    val tgtPrimaryKeys = jdbcConnector.getTablePrimaryKeys(jdbcDbSchemaOwner, targetTable)
    var joinConditions = ArrayBuffer[String]()
    val newFields = (tgtPrimaryKeys.toList diff List("BATCH_LOAD_EPOCH"))
    val conn = DriverManager.getConnection(jdbcUrl, connectionProperties)

    for (columns <- newFields) {
      joinConditions += ("A." + columns + " = B." + columns )
    }

      storedProc =
        s"""EXEC ${storedProcSchemaOwner}.LOAD_DATA_CLEANUP '${jdbcDbSchemaOwner}','${targetTable}','${newFields.mkString(",")}','${joinConditions.mkString(" AND ")}' """.stripMargin

    println(storedProc)
    val rs = conn.createStatement.executeQuery(storedProc)

    var exitCode = 0
    while(rs.next()) {
      if (rs.getString("STATUS") == "SUCCEEDED") {
        exitCode = 0
      }
      else {
        exitCode = 1
      }
    }
    conn.close()
    exitCode
  }

}