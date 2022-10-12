package com.br.datahub.generic.ingestion

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.File

trait SparkSessionTest extends TestUtils {
  val property = "java.io.tmpdir"
  val tempDir: String = System.getProperty(property)
  val tempWarehouse = tempDir + File.separator + "warehouse_datahub_generic_" + System.currentTimeMillis()
  new File(tempWarehouse).deleteOnExit()

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark Test")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.default.parallelism", "1")
      .config("spark.sql.warehouse.dir", tempWarehouse)
      .config("spark.local.dir", tempDir)
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext

  lazy implicit val sparkSession = spark

  def clearSparkCache(): Unit = {
    sparkSession.catalog.clearCache()
  }
}
