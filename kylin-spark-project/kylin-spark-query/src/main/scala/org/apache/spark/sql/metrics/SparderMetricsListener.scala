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

package org.apache.spark.sql.metrics

import org.apache.kylin.metrics.QuerySparkMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

class SparderMetricsListener() extends SparkListener with Logging {

  var stageJobMap:Map[Int, Int] = Map()
  var jobExecutionMap:Map[Int, QueryInformation] = Map()
  var executionInformationMap:Map[Long, ExecutionInformation] = Map()

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val executionIdString = event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    val sparderName = event.properties.getProperty("spark.app.name")
    val kylinQueryId = event.properties.getProperty("kylin.query.id")

    if (executionIdString == null) {
      // This is not a job created by SQL
      logInfo("Cannot happened.")
      return
    }

    val executionId = executionIdString.toLong

    if (executionInformationMap.apply(executionId).sparderName == null) {
      val executionInformation = new ExecutionInformation(kylinQueryId, executionInformationMap.apply(executionId).executionStartTime, sparderName)
      executionInformationMap += (executionId -> executionInformation)
    }

    jobExecutionMap += (event.jobId -> new QueryInformation(kylinQueryId, executionId))

    val stages = event.stageInfos.iterator
    while (stages.hasNext) {
      val stage: StageInfo = stages.next()
      stageJobMap += (stage.stageId -> event.jobId)
    }
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    val isSuccess = event.jobResult match {
      case JobSucceeded => true
      case _ => false
    }
    QuerySparkMetrics.addSparkJobMetrics(jobExecutionMap.apply(event.jobId).queryId, jobExecutionMap.apply(event.jobId).executionId, event.jobId, isSuccess)
    jobExecutionMap -= event.jobId
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stageInfo = event.stageInfo
    val isSuccess = stageInfo.getStatusString match {
      case "succeeded" => true
      case _ => false
    }
    val stageMetrics = stageInfo.taskMetrics
    QuerySparkMetrics.addSparkStageMetrics(stageJobMap.apply(stageInfo.stageId), stageInfo.stageId, stageInfo.name, isSuccess, stageMetrics.resultSize, stageMetrics.executorDeserializeCpuTime,
    stageMetrics.executorDeserializeCpuTime, stageMetrics.executorRunTime, stageMetrics.executorCpuTime, stageMetrics.jvmGCTime, stageMetrics.resultSerializationTime,
    stageMetrics.memoryBytesSpilled, stageMetrics.diskBytesSpilled, stageMetrics.peakExecutionMemory)
    stageJobMap -= stageInfo.stageId

    logInfo("Stage " + event.stageInfo.stageId + ", " + event.stageInfo.name + ", "
      + event.stageInfo.details + " is completed.")
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart => onQueryExecutionStart(e)
      case e: SparkListenerSQLExecutionEnd => onQueryExecutionEnd(e)
      case _ => // Ignore
    }
  }

  private def onQueryExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    executionInformationMap += (event.executionId -> new ExecutionInformation(-1, event.time, null))
  }

  private def onQueryExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val executionInformation = executionInformationMap.apply(event.executionId)
    QuerySparkMetrics.addExecutionMetrics(executionInformation.queryId, event.executionId, event.time - executionInformation.executionStartTime, executionInformation.sparderName)
    executionInformationMap -= event.executionId
    logInfo("QueryExecution " + event.executionId + " is completed at " + event.time)
    }
}

// ============================

private class ExecutionInformation (
                                   var queryId: String,
                                   var executionStartTime: Long,
                                   var sparderName: String
                                   )
private class QueryInformation (
                              val queryId: String,
                              val executionId: Long
                              )
