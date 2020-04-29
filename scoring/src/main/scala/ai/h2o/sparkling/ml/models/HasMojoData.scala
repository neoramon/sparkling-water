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

package ai.h2o.sparkling.ml.models

import java.io.File
import org.apache.hadoop.fs.Path

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkFiles

private[models] trait HasMojoData {

  var mojoFileName: String = null

  // Called during init of the model
  def distributeMojo(mojoPath: String): this.type = {
    val sparkSession = SparkSessionUtils.active
    val inputPath = new Path(mojoPath)
    val fs = inputPath.getFileSystem(SparkSessionUtils.active.sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    sparkSession.sparkContext.addFile(mojoPath)
    mojoFileName = new File(mojoPath).getName
    this
  }

  protected def getMojoLocalPath(): String = SparkFiles.get(mojoFileName)
}
