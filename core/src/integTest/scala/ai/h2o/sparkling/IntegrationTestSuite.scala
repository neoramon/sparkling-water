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

package ai.h2o.sparkling

import org.apache.spark.h2o.H2OConf
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntegrationTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local-cluster[2,1,2024]")

  test("Verify H2O cluster builds on local cluster") {
    val hc = H2OContext.getOrCreate(new H2OConf().setClusterSize(1))
    if (hc.getConf.runsInInternalClusterMode) {
      assert(hc.getH2ONodes().length == 2)
    } else {
      assert(hc.getH2ONodes().length == 1)
    }
  }

  test("Convert H2OFrame to DataFrame when H2OFrame was changed in DKV in distributed environment") {
    val rdd = sc.parallelize(1 to 100, 2)
    val h2oFrame = hc.asH2OFrame(rdd)
    assert(h2oFrame.chunks.length == 2)
    val updatedFrame = h2oFrame.add(h2oFrame)

    val convertedDf = hc.asSparkFrame(updatedFrame)
    convertedDf.collect()

    assert(convertedDf.count() == h2oFrame.numberOfRows)
    assert(convertedDf.columns.length == h2oFrame.columnNames.length)
  }

  test("H2OFrame High Availability: Task killed but frame still converted successfully") {
    val rdd = sc.parallelize(1 to 1000, 100).map(v => Some(v)).map { d =>
      import org.apache.spark.TaskContext
      val tc = TaskContext.get()
      if (tc.attemptNumber == 0) {
        throw new Exception("Failing first attempt!")
      } else {
        d
      }
    }

    val h2oFrame = hc.asH2OFrame(rdd)

    TestUtils.assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = rowIdx + 1
      val value = vec.at(rowIdx)
      assert(nextRowIdx == value, "The H2OFrame values should match row numbers+1")
    })

    h2oFrame.delete()
  }

  test("Spark Known Issues: PUBDEV-3808 - Spark's BroadcastHashJoin is non deterministic - Negative test") {
    val dataFile = getClass.getResource("/PUBDEV-3808_one_nullable_column.parquet").getFile
    val df = spark.read.parquet(dataFile).repartition(1).select("id", "strfeat0")

    val sampleA = df.sample(withReplacement = false, 0.1, seed = 0)
    val sampleB = df.sample(withReplacement = false, 0.1, seed = 0)

    // give it 10 attempts to observe the buggy behaviour
    val mismatch = (0 until 10).exists { _ =>
      val counts = (0 until 5).map(_ => sampleA.except(sampleB).count)
      // The elements shouldn't be the same in this case
      val first = counts.head
      counts.exists(c => c != first)
    }
    assert(mismatch, "The non-deterministic behaviour should be observable when BroadcastHashJoins are allowed")
  }

  test("Spark Known Issues: PUBDEV-3808 - Spark's BroadcastHashJoin is non deterministic - Positive test") {
    val dataFile = getClass.getResource("/PUBDEV-3808_one_nullable_column.parquet").getFile
    val df = spark.read.parquet(dataFile).repartition(1).select("id", "strfeat0")

    // disable BroadcastHashJoins
    spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
    val sampleA = df.sample(withReplacement = false, 0.1, seed = 0)
    val sampleB = df.sample(withReplacement = false, 0.1, seed = 0)

    val counts = (0 until 5).map(_ => sampleA.except(sampleB).count)
    // check whether all elements are the same
    val first = counts.head
    val mismatch = counts.exists(c => c != first)
    assert(!mismatch, "Number of elements in all samples should be the same since BroadcastHashJoins aren't used")
  }
}
