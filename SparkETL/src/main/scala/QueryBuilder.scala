package com.rh.bi.etl.spark

import scala.io._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scalaj.http._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import java.io._


object QueryBuilder {
  val etlFields = List("INSERTED_BY_USER_NAME", "INSERTED_UTC_DATETIME", "UPDATED_BY_USER_NAME", "UPDATED_UTC_DATETIME", "ETL_BATCH_SID","BATCH_LOAD_EPOCH")
  val scd2DateFields = List("EFFECTIVE_DATETIME", "EXPIRATION_DATETIME")
  val hashValueDelimiter="'*#*'"
  val columnListExtended=List("varchar","char","numeric")
  val currentIndicator_Field = List("CURRENT_IND")
  var batchEpoch: String = null


  def main(args: Array[String]): Unit = {
    val metadataFilename = args(0)
    val batchId = args(1)
    val outputPath = args(2)
    val jdbcDbSchemaOwner = args(3)
    val libPath = args(4)
    val etlBatchEpoch = args(5)

    var sqlQuery: String = null
    batchEpoch=etlBatchEpoch
    println(s"Reading JSON ${metadataFilename} ...")
    val jsonMetadataConfig = parseWithJackson(Source.fromFile(metadataFilename))

    /* val response = Http("https://demo8624007.mockable.io//lookup/")
      .header("key", "val").method("get")
      .execute().body.toString
println(response)
*/

    println(s"Getting Column Details for  ${jsonMetadataConfig.targetTable} ...")

    var exitCode = 1
    try {
      val tgtColumns = jdbcConnector.getTableColumn(jdbcDbSchemaOwner, jsonMetadataConfig.targetTable)
      val tgtColumnTypes = jdbcConnector.getTableColumnTypes(jdbcDbSchemaOwner, jsonMetadataConfig.targetTable)
      val tgtColumnTypesExtended = jdbcConnector.getTableColumnTypesExtended(jdbcDbSchemaOwner, jsonMetadataConfig.targetTable)
      val tgtPrimaryKeys = jdbcConnector.getTablePrimaryKeys(jdbcDbSchemaOwner, jsonMetadataConfig.targetTable)
      val delQuery = generateDeleteQuery(jsonMetadataConfig, tgtPrimaryKeys.toList)

      if (jsonMetadataConfig.logicalEntity == "DIM") {
        if (jsonMetadataConfig.scdType == "Type2") {
          sqlQuery = generateType2HybridQuery(jsonMetadataConfig, tgtColumns.toList)
          //  println("sqlType2HybridQuery:\n-------------------\n" + sqlQuery)
          //  sqlQuery = generateType1HybridQuery(jsonMetadataConfig, tgtColumns.toList)
          // println("sqlType1HybridQuery:\n-------------------\n" + sqlQuery)
        } else if (jsonMetadataConfig.scdType == "Type1") {
          sqlQuery = generateUpsertQuery(jsonMetadataConfig, tgtColumns.toList)
          //  println("sqlUpsertQuery:\n-------------------\n" + sqlQuery)
        }
      }
      else if (jsonMetadataConfig.logicalEntity == "FACT") {
        sqlQuery = generateInsertQuery(jsonMetadataConfig, tgtColumns.toList)
      }
      println(sqlQuery)
      printToFile(sqlQuery, outputPath)
      exitCode = 0

    }

    catch
      {
        case e: Exception => System.err.println("Exception caught, exit code: " + exitCode + "\n" + e);
          exitCode = 1

      }

    finally
    {
      sys.exit(exitCode)
    }
    sys.exit(exitCode)
}


  def parseWithJackson(json: BufferedSource): jsonPrepMetadata = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.readValue[jsonPrepMetadata](json.reader(),classOf[jsonPrepMetadata])
  }

  def printToFile(content: String, location: String ) =
  {
    val file = new File(location)
    file.createNewFile();
    val w = new PrintWriter(file)
    w.write(content)
    w.close()
  }


  def generateInsertQuery(jsonMetadataConfig: jsonPrepMetadata, tgtColumns: List[String]): String = {

    val allFields = tgtColumns
    val newFields = (allFields diff etlFields)

    val insertRecords: String =
      s"""SELECT
         |src.${newFields.mkString("\n,src.")},
         |'ETL' AS INSERTED_BY_USER_NAME,
         |current_timestamp AS INSERTED_UTC_DATETIME,
         |'ETL' AS UPDATED_BY_USER_NAME,
         |current_timestamp AS UPDATED_UTC_DATETIME,
         |src.ETL_BATCH_SID AS ETL_BATCH_SID,
         |${batchEpoch} AS BATCH_LOAD_EPOCH,
         |'I'    as action_flag
         |FROM REPLACE_ME_WITH_XFM_SCHEMAOWNER.${jsonMetadataConfig.sourceTable} src
         |WHERE   src.REPLACE_ME_WITH_PARTITION
         |""".stripMargin

    insertRecords
  }

  def generateUpsertQuery(jsonMetadataConfig: jsonPrepMetadata, tgtColumns: List[String]): String = {

    val allFields = tgtColumns
    val keys = List(jsonMetadataConfig.sidKey,jsonMetadataConfig.dimKey)

    val scdType1Fields = ( allFields diff etlFields )  diff keys

    val newFields = (allFields diff etlFields)

    val insertRecords: String =
      s"""SELECT
         |src.${newFields.mkString("\n,src.")},
         |'ETL' AS INSERTED_BY_USER_NAME,
         |current_timestamp AS INSERTED_UTC_DATETIME,
         |NULL AS UPDATED_BY_USER_NAME,
         |NULL AS UPDATED_UTC_DATETIME,
         |src.ETL_BATCH_SID AS ETL_BATCH_SID,
         |'I'    as action_flag
         |FROM REPLACE_ME_WITH_XFM_SCHEMAOWNER.${jsonMetadataConfig.sourceTable} src LEFT JOIN REPLACE_ME_WITH_LKP_SCHEMAOWNER.${jsonMetadataConfig.targetTable}  tgt
         |ON src.${jsonMetadataConfig.sidKey} = tgt.${jsonMetadataConfig.sidKey}
         |AND   tgt.REPLACE_ME_WITH_PARTITION AND src.REPLACE_ME_WITH_PARTITION
         |WHERE tgt.${jsonMetadataConfig.sidKey}  IS  NULL AND src.REPLACE_ME_WITH_PARTITION
         |""".stripMargin

    val updateRecords: String =
      s"""SELECT
         |src.${newFields.mkString("\n,src.")},
         |tgt.INSERTED_BY_USER_NAME AS INSERTED_BY_USER_NAME,
         |tgt.INSERTED_UTC_DATETIME AS INSERTED_UTC_DATETIME,
         |'ETL' AS UPDATED_BY_USER_NAME,
         |current_timestamp AS UPDATED_UTC_DATETIME,
         |src.ETL_BATCH_SID AS ETL_BATCH_SID,
         |'U'    as action_flag
         |FROM REPLACE_ME_WITH_XFM_SCHEMAOWNER.${jsonMetadataConfig.sourceTable} src LEFT JOIN REPLACE_ME_WITH_LKP_SCHEMAOWNER.${jsonMetadataConfig.targetTable}   tgt
         |ON src.${jsonMetadataConfig.sidKey} = tgt.${jsonMetadataConfig.sidKey}
         |AND   tgt.REPLACE_ME_WITH_PARTITION AND src.REPLACE_ME_WITH_PARTITION
         |WHERE tgt.${jsonMetadataConfig.sidKey}  IS NOT NULL AND src.REPLACE_ME_WITH_PARTITION
         |AND MD5(CONCAT(COALESCE(TRIM(UPPER(src.${scdType1Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(src.")})),''))) != MD5(CONCAT(COALESCE(TRIM(UPPER(tgt.${scdType1Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(tgt.")})),'')))
         |""".stripMargin

    insertRecords + " UNION ALL\n" + updateRecords
  }

  def generateType2HybridQuery(jsonMetadataConfig: jsonPrepMetadata, tgtColumns: List[String]): String = {

    val allFields = tgtColumns
   // val scdType1Fields = jsonMetadataConfig.scdType1Attributes
    val scdType2Fields = jsonMetadataConfig.scdType2Attributes

    val keys = List(jsonMetadataConfig.sidKey,jsonMetadataConfig.dimKey)

    val newFields = (((((allFields diff etlFields)  ) diff scdType2Fields) diff scd2DateFields) diff keys) diff currentIndicator_Field
    val scdType1Fields = ((((allFields diff etlFields) diff scdType2Fields) diff scd2DateFields) diff keys) diff currentIndicator_Field


    val insertRecords: String =
      s"""SELECT src.${jsonMetadataConfig.sidKey},
         |src.${jsonMetadataConfig.dimKey},
         | from_utc_timestamp(current_timestamp,'CST') + INTERVAL '1' SECOND as EFFECTIVE_DATETIME,
         |NULL as EXPIRATION_DATETIME,
         |'Y' as CURRENT_IND,
         |src.${scdType1Fields.mkString("\n,src.")},
         |src.${scdType2Fields.mkString("\n,src.")},
         |'ETL' AS INSERTED_BY_USER_NAME,
         |current_timestamp AS INSERTED_UTC_DATETIME,
         |NULL AS UPDATED_BY_USER_NAME,
         |NULL AS UPDATED_UTC_DATETIME,
         |src.ETL_BATCH_SID AS ETL_BATCH_SID,
         |'I'    as action_flag
         |FROM REPLACE_ME_WITH_XFM_SCHEMAOWNER.${jsonMetadataConfig.sourceTable} src LEFT JOIN REPLACE_ME_WITH_LKP_SCHEMAOWNER.${jsonMetadataConfig.targetTable}   tgt
         |ON src.${jsonMetadataConfig.dimKey} = tgt.${jsonMetadataConfig.dimKey}
         |AND   tgt.REPLACE_ME_WITH_PARTITION AND src.REPLACE_ME_WITH_PARTITION
         |WHERE
         |(( tgt.${jsonMetadataConfig.sidKey}  IS  NULL AND src.REPLACE_ME_WITH_PARTITION)
         |        or ( tgt.${jsonMetadataConfig.sidKey}  IS NOT NULL
         |             and tgt.CURRENT_IND = 'Y' AND src.REPLACE_ME_WITH_PARTITION
         |                        AND MD5(CONCAT(COALESCE(TRIM(UPPER(src.${scdType2Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(src.")})),''))) != MD5(CONCAT(COALESCE(TRIM(UPPER(tgt.${scdType2Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(tgt.")})),'')))
         |                        ) )
         |""".stripMargin

    val updateRecords2: String =
      s"""SELECT tgt.${jsonMetadataConfig.sidKey},
         |tgt.${jsonMetadataConfig.dimKey},
         |tgt.EFFECTIVE_DATETIME as EFFECTIVE_DATETIME,
         |tgt.EXPIRATION_DATETIME as EXPIRATION_DATETIME,
         |tgt.CURRENT_IND as CURRENT_IND,
         |src.${scdType1Fields.mkString("\n,src.")},
         |tgt.${scdType2Fields.mkString("\n,tgt.")},
         |tgt.INSERTED_BY_USER_NAME AS INSERTED_BY_USER_NAME,
         |tgt.INSERTED_UTC_DATETIME AS INSERTED_UTC_DATETIME,
         |'ETL' AS UPDATED_BY_USER_NAME,
         |current_timestamp AS UPDATED_UTC_DATETIME,
         |src.ETL_BATCH_SID AS ETL_BATCH_SID,
         |'U'    as action_flag
         |FROM REPLACE_ME_WITH_XFM_SCHEMAOWNER.${jsonMetadataConfig.sourceTable} src LEFT JOIN REPLACE_ME_WITH_LKP_SCHEMAOWNER.${jsonMetadataConfig.targetTable}   tgt
         |ON src.${jsonMetadataConfig.dimKey} = tgt.${jsonMetadataConfig.dimKey}
         |AND   tgt.REPLACE_ME_WITH_PARTITION AND src.REPLACE_ME_WITH_PARTITION
         |WHERE
         | ( tgt.${jsonMetadataConfig.sidKey}  IS NOT NULL
         | AND MD5(CONCAT(COALESCE(TRIM(UPPER(src.${scdType1Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(src.")})),''))) != MD5(CONCAT(COALESCE(TRIM(UPPER(tgt.${scdType1Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(tgt.")})),'')))
         |      AND tgt.CURRENT_IND = 'N' AND src.REPLACE_ME_WITH_PARTITION)
         |      OR ( tgt.${jsonMetadataConfig.sidKey}  IS NOT NULL
         |      AND MD5(CONCAT(COALESCE(TRIM(UPPER(src.${scdType1Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(src.")})),''))) != MD5(CONCAT(COALESCE(TRIM(UPPER(tgt.${scdType1Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(tgt.")})),'')))
         |      AND tgt.CURRENT_IND = 'Y' AND src.REPLACE_ME_WITH_PARTITION
         |      AND MD5(CONCAT(COALESCE(TRIM(UPPER(src.${scdType2Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(src.")})),''))) = MD5(CONCAT(COALESCE(TRIM(UPPER(tgt.${scdType2Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(tgt.")})),'')))
         |       )
         |""".stripMargin

    val updateRecords1: String =
      s"""SELECT tgt.${jsonMetadataConfig.sidKey},
       |tgt.${jsonMetadataConfig.dimKey},
       |tgt.EFFECTIVE_DATETIME as EFFECTIVE_DATETIME,
       | from_utc_timestamp(current_timestamp,'CST') as EXPIRATION_DATETIME,
       |'N' as CURRENT_IND,
       |src.${scdType1Fields.mkString("\n,src.")},
       |tgt.${scdType2Fields.mkString("\n,tgt.")},
       |tgt.INSERTED_BY_USER_NAME AS INSERTED_BY_USER_NAME,
       |tgt.INSERTED_UTC_DATETIME AS INSERTED_UTC_DATETIME,
       |'ETL' AS UPDATED_BY_USER_NAME,
       |current_timestamp AS UPDATED_UTC_DATETIME,
       |src.ETL_BATCH_SID AS ETL_BATCH_SID,
       |'U'    as action_flag
       |FROM REPLACE_ME_WITH_XFM_SCHEMAOWNER.${jsonMetadataConfig.sourceTable} src LEFT JOIN REPLACE_ME_WITH_LKP_SCHEMAOWNER.${jsonMetadataConfig.targetTable}   tgt
       |ON src.${jsonMetadataConfig.dimKey} = tgt.${jsonMetadataConfig.dimKey}
       |AND   tgt.REPLACE_ME_WITH_PARTITION AND src.REPLACE_ME_WITH_PARTITION
       |WHERE
       | ( tgt.${jsonMetadataConfig.sidKey}  IS NOT NULL
       |             and tgt.CURRENT_IND = 'Y' AND src.REPLACE_ME_WITH_PARTITION
                        AND MD5(CONCAT(COALESCE(TRIM(UPPER(src.${scdType2Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(src.")})),''))) != MD5(CONCAT(COALESCE(TRIM(UPPER(tgt.${scdType2Fields.mkString(")),'')," + hashValueDelimiter + ",COALESCE(TRIM(UPPER(tgt.")})),'')))
       |             )
       |""".stripMargin
    insertRecords + " UNION ALL\n" + updateRecords1 + " UNION ALL\n" + updateRecords2
  }


  def generateType1HybridQuery(jsonMetadataConfig: jsonPrepMetadata, tgtColumns: List[String]): String = {

    val allFields = tgtColumns
    val scdType1Fields = jsonMetadataConfig.scdType1Attributes
    val scdType2Fields = jsonMetadataConfig.scdType2Attributes
    val newFields = (((allFields diff etlFields) diff scdType1Fields ) diff scdType2Fields) diff scd2DateFields

    val updateRecords: String =
      s"""SELECT  DISTINCT tgt.${jsonMetadataConfig.dimKey},
         |src.${scdType1Fields.mkString("\n,src.")},
         |'ETL' AS UPDATED_BY_USER_NAME,
         |current_timestamp AS UPDATED_UTC_DATETIME,
         |src.ETL_BATCH_SID AS ETL_BATCH_SID,
         |'U'    as action_flag
         |FROM ${jsonMetadataConfig.sourceTable} src LEFT JOIN ${jsonMetadataConfig.targetTable}   tgt
         |ON src.${jsonMetadataConfig.dimKey} = tgt.${jsonMetadataConfig.dimKey}
         |WHERE
         |tgt.${jsonMetadataConfig.sidKey}  IS NOT NULL
         |AND MD5(CONCAT(src.${scdType1Fields.mkString(","+hashValueDelimiter+",src.")})) != MD5(CONCAT(tgt.${scdType1Fields.mkString(","+hashValueDelimiter+",tgt.")}))
         |""".stripMargin

   updateRecords
  }


  def generateDeleteQuery(jsonMetadataConfig: jsonPrepMetadata, tgtColumns: List[String]): String = {

    var joinConditions = ArrayBuffer[String]()
    val newFields = (tgtColumns diff List("BATCH_LOAD_EPOCH"))

    for (columns <- newFields) {
      joinConditions += ("A." + columns + " = B." + columns )
    }

    val deleteRecords: String =
      s""" DELETE
         | FROM ${jsonMetadataConfig.targetTable} A
         |LEFT JOIN (
         |SELECT MAX(BATCH_LOAD_EPOCH) MAX_BATCH_LOAD_EPOCH,
         |${newFields.mkString(",")}
         |FROM ${jsonMetadataConfig.targetTable}
         |GROUP BY ${newFields.mkString(",")} ) B
         |ON A.BATCH_LOAD_EPOCH = B.MAX_BATCH_LOAD_EPOCH AND
         | ${joinConditions.mkString(" AND ")}
         |WHERE B.MAX_BATCH_LOAD_EPOCH IS NULL
         |""".stripMargin
    deleteRecords
  }


}