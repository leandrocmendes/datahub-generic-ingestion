package com.br.datahub.generic.ingestion.interfaces

import com.br.datahub.generic.ingestion.model.{DataConfig, IngestionParameter}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait IngestionType extends Serializable {
  def readData(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession) : DataFrame
  def writeData(config: DataConfig, dfToInsert: DataFrame, mode:String)(implicit sparkSession: SparkSession): Unit
}
