package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.interfaces.IngestionType
import com.br.datahub.generic.ingestion.model.{DataConfig, IngestionParameter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object IngestionMongoService extends IngestionType with Logging{
  override def writeData(config: DataConfig, dfToInsert: DataFrame, mode:String)(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Write data on MongoDb collection: ${config.table}")

    dfToInsert
      .write
      .option("spark.mongodb.output.uri", config.uri)
      .option("spark.mongodb.output.database", config.database)
      .option("spark.mongodb.output.collection", config.table)
      .mode(mode)
      .format("com.mongodb.spark.sql.DefaultSource")
      .save()
  }

  override def readData(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): DataFrame = {
    throw new Exception("Only sources types JDBC, Parquet, Avro and Csv is implemented!")
  }
}
