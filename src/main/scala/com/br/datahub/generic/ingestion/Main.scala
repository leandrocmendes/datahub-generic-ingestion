package com.br.datahub.generic.ingestion

import com.br.datahub.generic.ingestion.model.IngestionParameter
import com.br.datahub.generic.ingestion.service.IngestionService
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


object Main extends App with Logging{
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("datahub-generic-ingestion")
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/test.myCollection")
    .getOrCreate();


  println("APP Name :" + spark.sparkContext.appName)
  println("Deploy Mode :" + spark.sparkContext.deployMode)
  println("Master :" + spark.sparkContext.master)

//  val propertiesFile = args(0)

  val yamlExample = "name: Leitura de csv de orders\nmode: APPEND\nowner: leandro costa\nsource:\n    typeIngestion: CSV\n    config:\n        path: /ingestion/bronze/orders.csv\n        separator: \";\"\n        header: true\n\ndestination:\n    typeIngestion: MYSQL\n    config:\n        host: ${MSSQL_HOST}\n        username: ${MSSQL_USERNAME}\n        password: ${MSSQL_PASSWORD}\n        table: Orders"

  def convertYamlToJson(yaml: String): String = {
    val yamlReader = new ObjectMapper(new YAMLFactory)
    val obj = yamlReader.readValue(yaml, classOf[Any])
    val jsonWriter = new ObjectMapper
    jsonWriter.writeValueAsString(obj)
  }

  println(convertYamlToJson(yamlExample))

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }

  val ingestionObject: IngestionParameter = fromJson[IngestionParameter](convertYamlToJson(yamlExample))

  println(ingestionObject)

  IngestionService.makeIngestion(ingestionObject)

}
