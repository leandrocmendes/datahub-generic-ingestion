package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.constants.ApplicationConstants
import com.br.datahub.generic.ingestion.model.IngestionParameter
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

object IngestionService {
  def doIngestion(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): Unit = {
    println("Starting method doIngestion ...")
    val df: DataFrame = {
      if(ingestionParameter.source.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.CSV){
        IngestionCsvService.readData(ingestionParameter)
      }else if(ingestionParameter.source.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.PARQUET){
        IngestionParquetService.readData(ingestionParameter)
      }else if(ingestionParameter.source.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.AVRO){
        IngestionAvroService.readData(ingestionParameter)
      }else if (ingestionParameter.source.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.JDBC){
        IngestionJdbcService.readData(ingestionParameter)
      }else{
        throw new Exception("Invalid source type")
      }
    }

    df.printSchema()
    df.show()

    if(ingestionParameter.destination.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.JDBC){
      IngestionJdbcService.writeData(ingestionParameter.destination.config,df, ingestionParameter.mode)
    }else if (ingestionParameter.destination.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.MONGODB){
      IngestionMongoService.writeData(ingestionParameter.destination.config, df, ingestionParameter.mode)
    }else{
      throw new Exception("Invalid destination type")
    }

    if(ingestionParameter.logConfig != null && ingestionParameter.logConfig.registerLog){
      generateLog(ingestionParameter)
    }else{
      println("Skipping generate log ...")
    }

    println("Finish method doIngestion ...")
  }

  def generateLog(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): Unit = {
    if(ingestionParameter.logConfig.typeConfig == null){
      throw new Exception("Configuration of log cannot be null!")
    }

    println("Start generate log ...")

    import sparkSession.implicits._

    val data = Seq((
      ingestionParameter.name,
      ingestionParameter.mode,
      ingestionParameter.owner,
      ingestionParameter.source.typeIngestion,
      ingestionParameter.destination.typeIngestion,
      LocalDateTime.now().toString
    ))

    val df = data.toDF(
      ApplicationConstants.LogColumns.NAME,
      ApplicationConstants.LogColumns.MODE,
      ApplicationConstants.LogColumns.OWNER,
      ApplicationConstants.LogColumns.SOURCE_TYPE,
      ApplicationConstants.LogColumns.DESTINATION_TYPE,
      ApplicationConstants.LogColumns.INGESTION_DATE
    )

    if (ingestionParameter.logConfig.typeConfig.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.JDBC) {
      IngestionJdbcService.writeData(ingestionParameter.logConfig.typeConfig.config, df, ApplicationConstants.IngestionMode.APPEND)
    } else if (ingestionParameter.logConfig.typeConfig.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.MONGODB) {
      IngestionMongoService.writeData(ingestionParameter.logConfig.typeConfig.config, df, ApplicationConstants.IngestionMode.APPEND)
    } else {
      throw new Exception("Invalid log config type")
    }

    println("Finish generate log ...")
  }
}
