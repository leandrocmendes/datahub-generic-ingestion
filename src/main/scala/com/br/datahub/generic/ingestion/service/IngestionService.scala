package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.model.IngestionParameter
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object IngestionService {
  def readCsv(path: String, separator: String, header: Boolean)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .read
      .option("header", header)
      .option("sep", separator)
      .csv(path)
  }

  def readParquet(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .read
      .parquet(path)
  }

  def readAvro(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession
      .read
      .format("avro")
      .load(path)
  }

  def readJdbc(host: String, user: String, password: String, tableName: String)(implicit sparkSession: SparkSession): DataFrame = {
    val prop = new Properties();
    prop.put("user", user)
    prop.put("password", password)

    sparkSession
      .read
      .jdbc(host, tableName,prop)
  }

  def writeJdbc(df:DataFrame, host: String, user: String, password: String, tableName: String)(implicit sparkSession: SparkSession): Unit = {
    val prop = new Properties()
    prop.put("driver", "com.mysql.cj.jdbc.Driver")
    prop.put("user", user)
    prop.put("password", password)

    df
      .write
      .jdbc(host, tableName, prop)
  }

  def writeMongoDB(df: DataFrame)(implicit sparkSession: SparkSession) = {
    df
    .write
    .format("mongodb")
    .save()
  }

  def makeIngestion(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): Unit = {
    val df: DataFrame = {
      if(ingestionParameter.source.typeIngestion.toLowerCase() == "csv"){
        readCsv(
          ingestionParameter.source.config.path,
          ingestionParameter.source.config.separator,
          ingestionParameter.source.config.header
        )
      }else if(ingestionParameter.source.typeIngestion.toLowerCase() == "parquet"){
        readParquet(
          ingestionParameter.source.config.path
        )
      }else if(ingestionParameter.source.typeIngestion.toLowerCase() == "avro"){
        readAvro(
          ingestionParameter.source.config.path
        )
      }else if (ingestionParameter.source.typeIngestion.toLowerCase() == "mysql"){
        readJdbc(
          ingestionParameter.source.config.host,
          ingestionParameter.source.config.username,
          ingestionParameter.source.config.password,
          ingestionParameter.source.config.table
        )
      }else{
        null
      }
    }

    df.printSchema()
    df.show()

    if(ingestionParameter.destination.typeIngestion.toLowerCase() == "mysql"){
      writeJdbc(
        df,
        ingestionParameter.destination.config.host,
        ingestionParameter.destination.config.username,
        ingestionParameter.destination.config.password,
        ingestionParameter.destination.config.table
      )
    }else if (ingestionParameter.destination.typeIngestion.toLowerCase() == "mongo"){
      writeMongoDB(
        df
      )
    }
  }
}
