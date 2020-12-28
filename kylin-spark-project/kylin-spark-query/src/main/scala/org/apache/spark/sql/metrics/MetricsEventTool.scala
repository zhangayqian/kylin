/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.metrics

import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{AccumulableInfo, SparkListenerEvent, SparkListenerExecutorMetricsUpdate, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, StageInfo}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

object MetricsEventTool extends Logging {

  def verbose(event: SparkListenerEvent): Unit = {

    event match {
      case jobEvent: SparkListenerJobStart => {
        val executionIdString = jobEvent.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)

        if (executionIdString == null) {
          // This is not a job created by SQL
          logInfo("Cannot happened.")
          return
        }
        var str = "ExecutionId -> " + executionIdString + " : "
        val stages = jobEvent.stageInfos.iterator
        while (stages.hasNext) {
          val stage: StageInfo = stages.next()
          val i = (stage.stageId, stage.name, stage.details, stage.numTasks, stage.rddInfos.toString())
          str += i.toString()
        }
        logInfo("Job " + jobEvent.jobId + " is started, info " + str)
        jobEvent.properties.list(System.out)
      }


      case stageEvent: SparkListenerStageSubmitted => {
        logInfo("Stage " + stageEvent.stageInfo.stageId + ", " + stageEvent.stageInfo.name + ", "
          + stageEvent.stageInfo.details + " is started.")
        stageEvent.properties.list(System.out)
      }


      case queryEvent: SparkListenerSQLExecutionStart => {
        val nodeName = queryEvent.sparkPlanInfo.nodeName
        val simpleString = queryEvent.sparkPlanInfo.simpleString
        val metadataString = queryEvent.sparkPlanInfo.metadata.mkString(", ")
        val metricsString = queryEvent.sparkPlanInfo.metrics.mkString(", ")
        logInfo("QueryExecution " + queryEvent.executionId + " is starting, desc is " + queryEvent.description
          + ", and detail is " + queryEvent.details + " , physicalPlanDescription is " + queryEvent.physicalPlanDescription + " .")
        logInfo("QueryExecution " + queryEvent.executionId + ", nodeName is " + nodeName + ", simpleString is " + simpleString
          + ", metadataString is " + metadataString + ", metricsString is " + metricsString)
      }


      case queryEvent: SparkListenerSQLExecutionEnd => {
        logInfo("QueryExecution " + queryEvent.executionId + " is completed at " + queryEvent.time)
      }


      case jobEvent: SparkListenerJobEnd => {
        logInfo("Job " + jobEvent.jobId + " is end, result is " + jobEvent.jobResult + " .")
      }


      case stageEvent: SparkListenerStageCompleted => {
        logInfo("Stage " + stageEvent.stageInfo.stageId + ", " + stageEvent.stageInfo.name + ", "
          + stageEvent.stageInfo.details + " is completed.")
        // Extend here !
        logInfo("All task returned " + stageEvent.stageInfo.taskMetrics.resultSize + " records .")
      }


      case metricsEvent: SparkListenerExecutorMetricsUpdate => {

        val updates: Seq[(Long, Int, Int, Seq[AccumulableInfo])] = metricsEvent.accumUpdates
        val metricsIterator = updates.iterator
        var str = ""
        while (metricsIterator.hasNext) {
          val m = metricsIterator.next()
          str += "Task = "
          str += m._1
          str += ", stage = "
          str += m._2
          str += ", attempt = "
          str += m._3
          str += ", info = "
          val updates = m._4.iterator
          while (updates.hasNext) {
            val i: AccumulableInfo = updates.next()
            str += i.name
            str += ","
            str += i.update
            str += ","
            str += i.value
            str += ","
            str += i.metadata
          }
        }

        logInfo("MetricsUpdate at executor " + metricsEvent.execId + ", details " + str)
      }

      case _ => logInfo("Unknown " + event.getClass + ".")
    }
  }
}
