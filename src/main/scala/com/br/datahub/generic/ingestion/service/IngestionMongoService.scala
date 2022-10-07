package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.interfaces.IngestionType
import com.br.datahub.generic.ingestion.model.IngestionParameter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object IngestionMongoService extends IngestionType with Logging{
  override def writeData(ingestionParameter: IngestionParameter, dfToInsert: DataFrame)(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Write data on MongoDb collection: ${ingestionParameter.destination.config.table}")

    dfToInsert
      .write
      .option("spark.mongodb.output.uri", ingestionParameter.destination.config.uri)
      .option("spark.mongodb.output.database", ingestionParameter.destination.config.database)
      .option("spark.mongodb.output.collection", ingestionParameter.destination.config.table)
      .mode(ingestionParameter.mode)
      .format("com.mongodb.spark.sql.DefaultSource")
      .save()
  }

  override def readData(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): DataFrame = {
    throw new Exception("Only sources types JDBC, Parquet, Avro and Csv is implemented!")
  }
}
