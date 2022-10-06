package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.interfaces.IngestionType
import com.br.datahub.generic.ingestion.model.IngestionParameter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object IngestionAvroService extends IngestionType with Logging{
  override def readData(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): DataFrame = {
    logInfo(s"Reading data from avro path: ${ingestionParameter.source.config.path}")

    sparkSession
      .read
      .format("avro")
      .load(ingestionParameter.source.config.path)
  }

  override def writeData(ingestionParameter: IngestionParameter, dfToInsert: DataFrame)(implicit sparkSession: SparkSession): Unit = {
    throw new Exception("Only destination type JDBC and MongoDb is implemented!")
  }
}
