#!/bin/bash

#/opt/spark/bin/spark-submit \
#--class com.br.datahub.generic.ingestion.Main \
#--master local \
#--name test \
#--files /opt/spark-data/ingestion-configuration-csv.yml \
#--jars /opt/spark-apps/mysql-connector-java-8.0.30.jar \
#./datahub-generic-ingestion-1.0-SNAPSHOT.jar ingestion-configuration-csv.yml

#/opt/spark/bin/spark-submit \
#--class com.br.datahub.generic.ingestion.Main \
#--master local \
#--files /opt/spark-data/ingestion-configuration-parquet.yml \
#--jars /opt/spark-apps/mysql-connector-java-8.0.30.jar \
#./datahub-generic-ingestion-1.0-SNAPSHOT.jar ingestion-configuration-parquet.yml

/opt/spark/bin/spark-submit \
--class com.br.datahub.generic.ingestion.Main \
--master local \
--name test-avro \
--files /opt/spark-data/ingestion-configuration-avro.yml \
--jars /opt/spark-apps/mysql-connector-java-8.0.30.jar \
--packages org.apache.spark:spark-avro_2.12:3.0.0 \
./datahub-generic-ingestion-1.0-SNAPSHOT.jar ingestion-configuration-avro.yml