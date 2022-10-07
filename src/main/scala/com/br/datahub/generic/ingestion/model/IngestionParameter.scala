package com.br.datahub.generic.ingestion.model
case class IngestionParameter(name: String, mode: String, owner: String, source: TypeConfig, destination: TypeConfig)
