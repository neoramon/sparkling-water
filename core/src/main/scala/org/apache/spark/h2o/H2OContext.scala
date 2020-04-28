/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.h2o

import java.util.concurrent.atomic.AtomicReference

import ai.h2o.sparkling.backend.H2OContextImplicits
import ai.h2o.sparkling.backend.converters.SparkDataFrameConverter
import ai.h2o.sparkling.backend.utils.{H2OClientUtils, RestApiUtils}
import org.apache.spark.expose.Logging
import org.apache.spark.sql.DataFrame
import water.DKV
import water.api.ImportHiveTableHandler.HiveTableImporter

import scala.language.{implicitConversions, postfixOps}


class H2OContext private (private val hc: ai.h2o.sparkling.H2OContext) {
  self =>

  def downloadH2OLogs(destinationDir: String, logContainer: String): String = {
    hc.downloadH2OLogs(destinationDir, logContainer)
  }

  def importHiveTable(
                       database: String = HiveTableImporter.DEFAULT_DATABASE,
                       table: String,
                       partitions: Array[Array[String]] = null,
                       allowMultiFormat: Boolean = false): Frame = {
    hc.importHiveTable(database, table, partitions, allowMultiFormat)
  }

  object implicits extends H2OContextImplicits with Serializable {
    protected override def _h2oContext: H2OContext = self
  }

  override def toString: String = hc.toString

  def openFlow(): Unit = hc.openFlow()

  def stop(stopSparkContext: Boolean = false): Unit = hc.stop(stopSparkContext)

  def flowURL(): String = hc.flowURL()

  def setH2OLogLevel(level: String): Unit = hc.setH2OLogLevel(level)

  def getH2OLogLevel(): String = hc.getH2OLogLevel()

  def h2oLocalClient: String = hc.h2oLocalClient

  def h2oLocalClientIp: String = hc.h2oLocalClientIp

  def h2oLocalClientPort: Int = hc.h2oLocalClientPort

  def asSparkFrame[T <: Frame](fr: T, copyMetadata: Boolean = true): DataFrame = {
    DKV.put(fr)
    SparkDataFrameConverter.toDataFrame(hc, ai.h2o.sparkling.H2OFrame(fr._key.toString), copyMetadata)
  }

  def asSparkFrame(s: String, copyMetadata: Boolean): DataFrame = {
    val frame = ai.h2o.sparkling.H2OFrame(s)
    SparkDataFrameConverter.toDataFrame(hc, frame, copyMetadata)
  }

  def asSparkFrame(s: String): DataFrame = asSparkFrame(s, copyMetadata = true)


}

object H2OContext extends Logging {

  private val instantiatedContext = new AtomicReference[H2OContext]()

  def getOrCreate(): H2OContext = {
    val hc = ai.h2o.sparkling.H2OContext.getOrCreate()
    instantiatedContext.set(new H2OContext(hc))
    instantiatedContext.get()
  }

  def getOrCreate(conf: H2OConf): H2OContext = synchronized {
    val hc = ai.h2o.sparkling.H2OContext.getOrCreate(conf)
    instantiatedContext.set(new H2OContext(hc))
    instantiatedContext.get()
  }

  def get(): Option[H2OContext] = Option(instantiatedContext.get())

  def ensure(onError: => String = "H2OContext has to be running."): H2OContext = {
    Option(instantiatedContext.get()) getOrElse {
      throw new RuntimeException(onError)
    }
  }
}
