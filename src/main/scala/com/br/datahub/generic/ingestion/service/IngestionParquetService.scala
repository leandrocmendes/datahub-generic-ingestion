package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.interfaces.IngestionType
import com.br.datahub.generic.ingestion.model.{DataConfig, IngestionParameter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object IngestionParquetService extends IngestionType with Logging{
  override def readData(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): DataFrame = {
    logInfo(s"Reading data from parquet path: ${ingestionParameter.source.config.path}")

    sparkSession
      .read
      .parquet(ingestionParameter.source.config.path)
  }

  override def writeData(config: DataConfig, dfToInsert: DataFrame, mode:String)(implicit sparkSession: SparkSession): Unit = {
    throw new Exception("Only destination type JDBC and MongoDb is implemented!")
  }
}
