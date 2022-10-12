package com.br.datahub.generic.ingestion

import org.apache.spark.internal.Logging

import scala.reflect.io

trait TestUtils extends Logging {
  def getFullPathFromTarget(relativeFileName: String): String = {
    io.File("target/test-classes" + relativeFileName).toAbsolute.toString()
  }

}
