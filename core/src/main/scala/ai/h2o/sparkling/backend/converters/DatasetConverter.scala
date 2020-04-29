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

package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.H2OContext
import org.apache.spark.expose.Logging
import org.apache.spark.sql.Dataset

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.runtime.universe._

object DatasetConverter extends Logging {

  def toH2OFrame[T <: Product](hc: H2OContext, ds: Dataset[T], frameKeyName: Option[String])(
      implicit ttag: TypeTag[T]) = {
    SparkDataFrameConverter.toH2OFrame(hc, ds.toDF(), frameKeyName)
  }
}
