package com.rh.bi.etl.spark

case class jsonPrepMetadata(
                         logicalEntity: String,
                         scdType: String,
                         processType: String,
                         sidKey: String,
                         dimKey: String,
                         primaryKey: List[String],
                         scdType1Attributes: List[String],
                         scdType2Attributes: List[String],
                         sourceTable: String,
                         targetTable: String,
                         processSQL: String,
                         sidLookup: List[String]
                       )