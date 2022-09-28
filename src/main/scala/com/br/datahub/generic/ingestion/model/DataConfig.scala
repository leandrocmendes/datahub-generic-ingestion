package com.br.datahub.generic.ingestion.model

case class DataConfig(host: String = "", username: String = "", password: String = "", table: String = "", path: String = "", separator: String = "", header: Boolean = true)
