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
package ai.h2o.sparkling.backend.api

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.api.dataframes.DataFramesHandler
import ai.h2o.sparkling.backend.api.h2oframes.H2OFramesHandler
import ai.h2o.sparkling.backend.api.rdds.RDDsHandler
import ai.h2o.sparkling.backend.api.scalainterpreter.ScalaCodeHandler
import water.api.RestApiContext

/**
  * Sparkling Water Core REST API
  */
object CoreRestAPI extends RestApi {

  override def registerEndpoints(hc: H2OContext, context: RestApiContext): Unit = {
    if (hc.getConf.isH2OReplEnabled) {
      ScalaCodeHandler.registerEndpoints(context, hc.sparkContext, hc)
    }
    DataFramesHandler.registerEndpoints(context, hc.sparkContext, hc)
    H2OFramesHandler.registerEndpoints(context, hc.sparkContext, hc)
    RDDsHandler.registerEndpoints(context, hc.sparkContext, hc)
  }

  override def name: String = "Core Sparkling Water Rest API"
}
