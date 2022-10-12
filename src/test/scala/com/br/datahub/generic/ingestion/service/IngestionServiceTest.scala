package com.br.datahub.generic.ingestion.service

import com.br.datahub.generic.ingestion.SparkSessionTest
import com.br.datahub.generic.ingestion.model.{DataConfig, IngestionParameter, TypeConfig}
import com.github.simplyscala.{MongoEmbedDatabase, MongodProps}
import org.junit.{AfterClass, Assert, BeforeClass, Test}

import java.sql.DriverManager
import java.util.Properties

object IngestionServiceTest extends MongoEmbedDatabase {
  var mongoInstance: MongodProps = null

  @BeforeClass
  def setup(): Unit = {
    try {
      mongoInstance = mongoStart(27018)
    }
    catch {
      case ex: Exception =>
    }
  }

  @AfterClass
  def teardown(): Unit = {
    mongoStop(mongoInstance)
  }
}

class IngestionServiceTest extends SparkSessionTest {

  @Test
  def testCsvToJdbc{
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user","user")
    connectionProperties.setProperty("password", "pw")

    val url = "jdbc:h2:mem:dbo;MODE=MYSQL"

    val conn = DriverManager.getConnection(url,connectionProperties)
    conn.prepareStatement("create table if not exists MoviesCsv(movieId int, title varchar(255), genres varchar(255));")
    conn.commit()

    val pathProcess: String = getFullPathFromTarget("/massa_csv")

    val ingestionConf: IngestionParameter = IngestionParameter(
      name = "Test Ingestion CSV To MySQL",
      mode = "OVERWRITE",
      owner = "Teste",
      source = TypeConfig(
        typeIngestion = "CSV",
        config = DataConfig(
          path = s"$pathProcess/movies.csv",
          separator = ","
        )
      ),
      destination = TypeConfig(
        typeIngestion = "JDBC",
        config = DataConfig(
          host = "jdbc:h2:mem:dbo;MODE=MYSQL",
          username = "user",
          password = "pw",
          table = "MoviesCsv",
          driver = "org.h2.Driver"
        )
      )
    )

    IngestionService.doIngestion(ingestionConf)

    val df = sparkSession.read.jdbc(url,"MoviesCsv",connectionProperties)

    Assert.assertTrue(df.count() == 30)
  }

  @Test
  def testParquetToJdbc {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "user")
    connectionProperties.setProperty("password", "pw")

    val url = "jdbc:h2:mem:dbo;MODE=MYSQL"

    val conn = DriverManager.getConnection(url, connectionProperties)
    conn.prepareStatement("create table if not exists MoviesParquet(movieId int, title varchar(255), genres varchar(255));")
    conn.commit()

    val pathProcess: String = getFullPathFromTarget("/massa_parquet")

    val ingestionConf: IngestionParameter = IngestionParameter(
      name = "Test Ingestion PARQUET To MySQL",
      mode = "OVERWRITE",
      owner = "Teste",
      source = TypeConfig(
        typeIngestion = "PARQUET",
        config = DataConfig(
          path = pathProcess,
        )
      ),
      destination = TypeConfig(
        typeIngestion = "JDBC",
        config = DataConfig(
          host = "jdbc:h2:mem:dbo;MODE=MYSQL",
          username = "user",
          password = "pw",
          table = "MoviesParquet",
          driver = "org.h2.Driver"
        )
      )
    )

    IngestionService.doIngestion(ingestionConf)

    val df = sparkSession.read.jdbc(url, "MoviesParquet", connectionProperties)

    Assert.assertTrue(df.count() == 30)
  }

  @Test
  def testAvroToJdbc {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "user")
    connectionProperties.setProperty("password", "pw")

    val url = "jdbc:h2:mem:dbo;MODE=MYSQL"

    val conn = DriverManager.getConnection(url, connectionProperties)
    conn.prepareStatement("create table if not exists MoviesAvro(movieId int, title varchar(255), genres varchar(255));")
    conn.commit()

    val pathProcess: String = getFullPathFromTarget("/massa_avro/orders.avro")

    val ingestionConf: IngestionParameter = IngestionParameter(
      name = "Test Ingestion AVRO To MySQL",
      mode = "OVERWRITE",
      owner = "Teste",
      source = TypeConfig(
        typeIngestion = "AVRO",
        config = DataConfig(
          path = pathProcess,
        )
      ),
      destination = TypeConfig(
        typeIngestion = "JDBC",
        config = DataConfig(
          host = "jdbc:h2:mem:dbo;MODE=MYSQL",
          username = "user",
          password = "pw",
          table = "MoviesAvro",
          driver = "org.h2.Driver"
        )
      )
    )

    IngestionService.doIngestion(ingestionConf)

    val df = sparkSession.read.jdbc(url, "MoviesAvro", connectionProperties)

    Assert.assertTrue(df.count() == 30)
  }

  @Test
  def testAvroToMongoDB {
    val pathProcess: String = getFullPathFromTarget("/massa_avro/orders.avro")

    val ingestionConf: IngestionParameter = IngestionParameter(
      name = "Test Ingestion AVRO To MongoDb",
      mode = "OVERWRITE",
      owner = "Teste",
      source = TypeConfig(
        typeIngestion = "AVRO",
        config = DataConfig(
          path = pathProcess,
        )
      ),
      destination = TypeConfig(
        typeIngestion = "MONGODB",
        config = DataConfig(
          uri = "mongodb://localhost:27018/?authSource=admin",
          table = "MoviesAvro",
          database = "test"
        )
      )
    )

    IngestionService.doIngestion(ingestionConf)

    val df = sparkSession
      .read
      .option("spark.mongodb.input.uri", ingestionConf.destination.config.uri)
      .option("spark.mongodb.input.database", ingestionConf.destination.config.database)
      .option("spark.mongodb.input.collection", ingestionConf.destination.config.table)
      .format("com.mongodb.spark.sql.DefaultSource")
      .load()

    Assert.assertTrue(df.count() == 30)
  }

  @Test
  def testJdbcToMongoDB {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "user")
    connectionProperties.setProperty("password", "pw")

    val url = "jdbc:h2:mem:dbo;MODE=MYSQL"

    val conn = DriverManager.getConnection(url, connectionProperties)
    conn.prepareStatement("create table if not exists MoviesAvro2(movieId int, title varchar(255), genres varchar(255));")
    conn.commit()

    val pathProcess: String = getFullPathFromTarget("/massa_avro/orders.avro")

    var ingestionConf: IngestionParameter = IngestionParameter(
      name = "Test Ingestion AVRO To MySQL",
      mode = "OVERWRITE",
      owner = "Teste",
      source = TypeConfig(
        typeIngestion = "AVRO",
        config = DataConfig(
          path = pathProcess,
        )
      ),
      destination = TypeConfig(
        typeIngestion = "JDBC",
        config = DataConfig(
          host = "jdbc:h2:mem:dbo;MODE=MYSQL",
          username = "user",
          password = "pw",
          table = "MoviesAvro2",
          driver = "org.h2.Driver"
        )
      )
    )

    IngestionService.doIngestion(ingestionConf)


    ingestionConf = IngestionParameter(
      name = "Test Ingestion MySQL to MongoDB",
      mode = "OVERWRITE",
      owner = "Teste",
      source = TypeConfig(
        typeIngestion = "JDBC",
        config = DataConfig(
          host = "jdbc:h2:mem:dbo;MODE=MYSQL",
          username = "user",
          password = "pw",
          table = "MoviesAvro2",
          driver = "org.h2.Driver"
        )
      ),
      destination = TypeConfig(
        typeIngestion = "MONGODB",
        config = DataConfig(
          uri = "mongodb://localhost:27018/?authSource=admin",
          table = "MoviesJdbc",
          database = "test"
        )
      )
    )

    IngestionService.doIngestion(ingestionConf)

    val df = sparkSession
      .read
      .option("spark.mongodb.input.uri", ingestionConf.destination.config.uri)
      .option("spark.mongodb.input.database", ingestionConf.destination.config.database)
      .option("spark.mongodb.input.collection", ingestionConf.destination.config.table)
      .format("com.mongodb.spark.sql.DefaultSource")
      .load()

    Assert.assertTrue(df.count() == 30)
  }
}
