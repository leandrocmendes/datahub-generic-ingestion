#!/bin/bash

#/opt/spark/bin/spark-submit \
#--class com.br.datahub.generic.ingestion.Main \
#--master local \
#--name test \
#--files /opt/spark-data/ingestion-configuration-csv-to-mysql.yml \
#--jars /opt/spark-apps/mysql-connector-java-8.0.30.jar \
#./datahub-generic-ingestion-1.0-SNAPSHOT.jar ingestion-configuration-csv-to-mysql.yml

#/opt/spark/bin/spark-submit \
#--class com.br.datahub.generic.ingestion.Main \
#--master local \
#--files /opt/spark-data/ingestion-configuration-parquet-to-mysql.yml \
#--jars /opt/spark-apps/mysql-connector-java-8.0.30.jar \
#./datahub-generic-ingestion-1.0-SNAPSHOT.jar ingestion-configuration-parquet-to-mysql.yml

#/opt/spark/bin/spark-submit \
#--class com.br.datahub.generic.ingestion.Main \
#--master local \
#--name test-avro \
#--files /opt/spark-data/ingestion-configuration-avro-to-mysql.yml \
#--jars /opt/spark-apps/mysql-connector-java-8.0.30.jar \
#--packages org.apache.spark:spark-avro_2.12:3.0.0 \
#./datahub-generic-ingestion-1.0-SNAPSHOT.jar ingestion-configuration-avro-to-mysql.yml

/opt/spark/bin/spark-submit \
--class com.br.datahub.generic.ingestion.Main \
--name test-avro-to-mongo \
--conf "spark.mongodb.output.uri=mongodb://root:admin@spark_mongo_1:27017/?authSource=admin" \
--conf "spark.mongodb.output.database=test" \
--conf "spark.mongodb.output.collection=OrdersAvro" \
--packages org.apache.spark:spark-avro_2.12:3.0.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
--files /opt/spark-data/ingestion-configuration-avro-to-mongodb.yml \
./datahub-generic-ingestion-1.0-SNAPSHOT.jar ingestion-configuration-avro-to-mongodb.yml