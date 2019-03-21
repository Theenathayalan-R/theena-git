package com.rh.bi.etl.spark
import scala.io._

import com.fasterxml.jackson.databind.{ObjectMapper,DeserializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.hive._
import com.databricks.spark.avro._
import org.apache.hadoop.fs._

object dataPrepService {

  def parseWithJackson(json: BufferedSource): jsonPrepMetadata = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.readValue[jsonPrepMetadata](json.reader())
  }

  def generateUpsertQuery(jsonMetadataConfig: jsonPrepMetadata, tgtColumns: List[String]): String = {

    val allFields = tgtColumns
    val scdType1Fields = jsonMetadataConfig.scdType1Attributes
   val etlFields = List("INSERTED_BY_USER_NAME","INSERTED_UTC_DATETIME","UPDATED_BY_USER_NAME","UPDATED_UTC_DATETIME","ETL_BATCH_SID")
    //val etlFields = List("CREATED_BY_USER_NAME","CREATED_UTC_DATETIME","UPDATED_BY_USER_NAME","UPDATED_UTC_DATETIME","ETL_BATCH_SID")
    val newFields=(allFields  diff etlFields)

    val insertRecords: String =
      s"""SELECT
         |src.${newFields.mkString("\n,src.")},
         |'ETL' AS INSERTED_BY_USER_NAME,
         |current_timestamp AS INSERTED_UTC_DATETIME,
         |'' AS UPDATED_BY_USER_NAME,
         |'' AS UPDATED_UTC_DATETIME,
         |1 AS ETL_BATCH_SID
         |FROM sourceTable src LEFT JOIN targetTable  tgt
         |ON src.${jsonMetadataConfig.sidKey} = tgt.${jsonMetadataConfig.sidKey}
         |WHERE tgt.${jsonMetadataConfig.sidKey}  IS  NULL
         |""".stripMargin

    val updateRecords: String =
      s"""SELECT
         |src.${newFields.mkString("\n,src.")},
         |tgt.INSERTED_BY_USER_NAME AS INSERTED_BY_USER_NAME,
         |tgt.INSERTED_UTC_DATETIME AS INSERTED_UTC_DATETIME,
         |'ETL' AS UPDATED_BY_USER_NAME,
         |current_timestamp AS UPDATED_UTC_DATETIME,
         |1 AS ETL_BATCH_SID
         |FROM sourceTable  src LEFT JOIN targetTable  tgt
         |ON src.${jsonMetadataConfig.sidKey} = tgt.${jsonMetadataConfig.sidKey}
         |WHERE tgt.${jsonMetadataConfig.sidKey}  IS NOT NULL
         |""".stripMargin

  /*  val updateRecords: String =
   s"""SELECT src.${jsonMetadataConfig.sidKey},
      |src.${jsonMetadataConfig.naturalKey.mkString},
      |src.${newFields.mkString("\n,src.")},
      |src.${scdType1Fields.mkString("\n,src.")},
      |src.INSERTED_BY_USER_NAME AS INSERTED_BY_USER_NAME,
      |src.INSERTED_UTC_DATETIME AS INSERTED_UTC_DATETIME,
      |'ETL' AS UPDATED_BY_USER_NAME,
      |current_timestamp AS UPDATED_UTC_DATETIME,
      |1 AS ETL_BATCH_SID,
      |FROM src src LEFT JOIN tgt  tgt
      |ON src.${jsonMetadataConfig.sidKey} = tgt.${jsonMetadataConfig.sidKey}
      |WHERE tgt.${jsonMetadataConfig.sidKey}  IS NOT NULL
      |AND MD5(CONCAT(src.${scdType1Fields.mkString(",'*#*',src.")})) <> MD5(CONCAT(tgt.${scdType1Fields.mkString(",'*#*',tgt.")}))
      |""".stripMargin
      */

    insertRecords+" UNION \n" + updateRecords
  }


  def main(args: Array[String]): Unit = {
    val metadataFilename = args(0)
    val avroInputPath = args(1)
    val avroOutputPath = args(2)
    val batchId = args(3)


    println(s"Reading JSON ${metadataFilename} ...")
    val jsonMetadataConfig = parseWithJackson(Source.fromFile(metadataFilename))

    val spark = SparkSession
      .builder()
      .appName("dataPrepService")
      .master("local[*]")
      .config("spark.driver.extraJavaOptions", "-Dderby.system.home=file:///tmp/spark_warehouse/derby")
      .config("spark.sql.warehouse.dir", "file:///tmp/spark_warehouse/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate();
    import spark.implicits._
    import spark.sql
    //println(spark.conf.getAll.mkString("\n"))
    //sql("CREATE TABLE IF NOT EXISTS src3 (key INT, value STRING)")

    val jdbcHostname = "sdbicogazsdbsx002.database.windows.net"
    val jdbcPort = 1433
    val jdbcDatabase ="SDBIDBSSQLX001"
    val jdbcUsername="NifiETL"
    val jdbcPassword="Nifi_ETL_12345"
    val jdbcDbSchemaOwner="BI_ETL_METADATA"
    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
    val connectionProperties = new java.util.Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    connectionProperties.setProperty("Driver", driverClass)
    connectionProperties.setProperty("AutoCommit", "true")
    val fqTablename = jdbcDbSchemaOwner + "." + jsonMetadataConfig.targetTable
    val fqTmpTablename = jdbcDbSchemaOwner + ".TMP_" + jsonMetadataConfig.targetTable

    val tgt = spark.read.jdbc(jdbcUrl, fqTablename, connectionProperties)
    tgt.registerTempTable("targetTable")
    val tgtColumns : Array[String] =  tgt.columns
   // println(tgtColumns.toList)
    val tgtColumnsDatatypes : Array[scala.Tuple2[String,String]] =  tgt.dtypes
    //tgtColumnsDatatypes.foreach(println)
   // jsonMetadataConfig.schema=tgtColumns
    val sqlQuery= generateUpsertQuery(jsonMetadataConfig,tgtColumns.toList)
    //println(sqlQuery)
    val df = spark.read.avro(avroInputPath)
    df.registerTempTable("sourceTable")
   //df.show()
  //  spark.sql("show tables").show()
 //spark.sql(sqlQuery).show()
  spark.sql(sqlQuery).write.mode(SaveMode.Append).jdbc(jdbcUrl, fqTablename, connectionProperties)
    val insertQuery: String =
      s"""SELECT ${tgtColumns.toList.mkString(",")}
         |FROM ${fqTmpTablename}
         |WHERE ACTION_FLAG ='I'
         |""".stripMargin
    println(insertQuery)
  }
}