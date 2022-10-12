package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.interfaces.IngestionType
import com.br.datahub.generic.ingestion.model.{DataConfig, IngestionParameter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object IngestionJdbcService extends IngestionType with Logging{
  override def readData(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): DataFrame = {
    logInfo(s"Reading data from jdbc table: ${ingestionParameter.source.config.table}")

    val prop = new Properties()
    prop.put("driver", ingestionParameter.source.config.driver)
    prop.put("user", ingestionParameter.source.config.username)
    prop.put("password", ingestionParameter.source.config.password)

    sparkSession
      .read
      .jdbc(
        ingestionParameter.source.config.host,
        ingestionParameter.source.config.table,
        prop
      )
  }

  override def writeData(config: DataConfig, dfToInsert: DataFrame, mode:String)(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Write data on jdbc table: ${config.table}")

    val prop = new Properties()
    prop.put("driver", config.driver)
    prop.put("user", config.username)
    prop.put("password", config.password)

    dfToInsert
      .write
      .mode(mode)
      .jdbc(
        config.host,
        config.table,
        prop
      )
  }
}
