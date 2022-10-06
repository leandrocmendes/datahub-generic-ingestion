package com.br.datahub.generic.ingestion.interfaces

import com.br.datahub.generic.ingestion.model.IngestionParameter
import org.apache.spark.sql.{DataFrame, SparkSession}

trait IngestionType extends Serializable {
  def readData(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession) : DataFrame
  def writeData(ingestionParameter: IngestionParameter, dfToInsert: DataFrame)(implicit sparkSession: SparkSession): Unit
}
