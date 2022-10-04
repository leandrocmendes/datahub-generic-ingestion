package com.br.datahub.generic.ingestion

import com.br.datahub.generic.ingestion.constants.ApplicationConstants
import com.br.datahub.generic.ingestion.model.IngestionParameter
import com.br.datahub.generic.ingestion.service.IngestionService
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.SparkFiles

import scala.io.Source

object Main extends App with Logging{
  implicit var spark: SparkSession = getSparkSession()

  println("Starting Ingestion Pipeline...")
  println("App Name :" + spark.sparkContext.appName)

  try{
    val propertiesFile = args(0)

    val yamlConfiguration = SparkFiles.get(propertiesFile)

    val strYaml = Source.fromFile(yamlConfiguration).mkString

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
      mapper.readValue[T](json)
    }

    val ingestionObject: IngestionParameter = fromJson[IngestionParameter](convertYamlToJson(strYaml))

    spark = getSparkSession(ingestionObject)

    IngestionService.makeIngestion(ingestionObject)
  }catch {
    case ex: Exception =>
      logError("Job Failed")
      ex.printStackTrace()
      throw ex
  }
  def convertYamlToJson(yaml: String): String = {
    val yamlReader = new ObjectMapper(new YAMLFactory)
    val obj = yamlReader.readValue(yaml, classOf[Any])
    val jsonWriter = new ObjectMapper
    jsonWriter.writeValueAsString(obj)
  }

  def getSparkSession(ingestionObject: IngestionParameter = null): SparkSession = {
    val spark = if(ingestionObject == null) {
      SparkSession.builder().enableHiveSupport().getOrCreate()
    } else{
      if(ingestionObject.destination.typeIngestion.toLowerCase.equals(ApplicationConstants.IngestionTypes.MONGODB)){
        SparkSession.builder()
          .config("spark.mongodb.output.uri", ingestionObject.destination.config.uri)
          .config("spark.mongodb.output.database", ingestionObject.destination.config.database)
          .config("spark.mongodb.output.collection", ingestionObject.destination.config.table)
          .enableHiveSupport()
          .getOrCreate()
      }else{
        SparkSession.builder().enableHiveSupport().getOrCreate()
      }
    }
    spark
  }
}
