package com.br.datahub.generic.ingestion.constants

object ApplicationConstants {
  object IngestionTypes{
    val CSV = "csv"
    val PARQUET = "parquet"
    val AVRO = "avro"
    val JDBC= "jdbc"
    val MONGODB = "mongodb"
  }

  object LogColumns{
    val NAME = "name"
    val MODE = "mode"
    val OWNER = "owner"
    val SOURCE_TYPE =  "source_type"
    val DESTINATION_TYPE = "destination_type"
    val INGESTION_DATE = "ingestion_date"
  }

  object IngestionMode{
    val OVERWRITE = "OVERWRITE"
    val APPEND = "APPEND"
  }

}
