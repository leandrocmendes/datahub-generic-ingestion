package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.constants.ApplicationConstants
import com.br.datahub.generic.ingestion.model.IngestionParameter
import org.apache.spark.sql.{DataFrame, SparkSession}

object IngestionService {
  def doIngestion(ingestionParameter: IngestionParameter)(implicit sparkSession: SparkSession): Unit = {
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
      IngestionJdbcService.writeData(ingestionParameter,df)
    }else if (ingestionParameter.destination.typeIngestion.toLowerCase() == ApplicationConstants.IngestionTypes.MONGODB){
      IngestionMongoService.writeData(ingestionParameter, df)
    }else{
      throw new Exception("Invalid destination type")
    }
  }
}
