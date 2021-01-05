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

package org.apache.kylin.rest.metrics;

import java.nio.charset.Charset;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.QuerySparkMetrics;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimedRecordEvent;
import org.apache.kylin.metrics.property.*;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;

import org.apache.kylin.shaded.com.google.common.hash.HashFunction;
import org.apache.kylin.shaded.com.google.common.hash.Hashing;

/**
 * The entrance of metrics features.
 */
@ThreadSafe
public class QueryMetricsFacade {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsFacade.class);
    private static final HashFunction hashFunc = Hashing.murmur3_128();

    private static boolean enabled = false;
    private static ConcurrentHashMap<String, QueryMetrics> metricsMap = new ConcurrentHashMap<>();

    public static void init() {
        enabled = KylinConfig.getInstanceFromEnv().getQueryMetricsEnabled();
        if (!enabled)
            return;

        DefaultMetricsSystem.initialize("Kylin");
    }

    private static long getSqlHashCode(String sql) {
        return hashFunc.hashString(sql, Charset.forName("UTF-8")).asLong();
    }

    public static void updateMetrics(String queryId, SQLRequest sqlRequest, SQLResponse sqlResponse) {
        updateMetricsToLocal(sqlRequest, sqlResponse);
        updateMetricsToReservoir(queryId, sqlRequest, sqlResponse);
    }

    private static void updateMetricsToLocal(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (!enabled)
            return;

        String projectName = sqlRequest.getProject();
        update(getQueryMetrics("Server_Total"), sqlResponse);
        update(getQueryMetrics(projectName), sqlResponse);

        String cube = sqlResponse.getCube();
        String cubeName = cube.replace("=", "->");
        String cubeMetricName = projectName + ",sub=" + cubeName;
        update(getQueryMetrics(cubeMetricName), sqlResponse);
    }

    /**
     * report query related metrics
     */
    private static void updateMetricsToReservoir(String queryId, SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (!KylinConfig.getInstanceFromEnv().isKylinMetricsReporterForQueryEnabled()) {
            return;
        }
        String user = SecurityContextHolder.getContext().getAuthentication().getName();
        if (user == null) {
            user = "unknown";
        }


        QuerySparkMetrics.QueryExecutionMetrics queryExecutionMetrics = QuerySparkMetrics.getQueryExecutionMetricsMap().get(queryId);
        if (queryExecutionMetrics != null) {
            RecordEvent queryMetricsEvent = new TimedRecordEvent(
                    KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQuery());
            setSparkExecutionWrapper(queryMetricsEvent, user, sqlRequest.getSql(),
                    sqlResponse.isStorageCacheUsed() ? "CACHE" : "PARQUET", queryId, queryExecutionMetrics.getExecutionId(),
                    sqlRequest.getProject(), sqlResponse.getCube(), sqlResponse.getCuboidIds(), sqlResponse.getThrowable());

            //setSparkExecutionMetrics();
        }

        /*

        for (QueryContext.RPCStatistics entry : QueryContextFacade.current().getRpcStatisticsList()) {
            RecordEvent rpcMetricsEvent = new TimedRecordEvent(
                    KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryRpcCall());
            setRPCWrapper(rpcMetricsEvent, //
                    norm(sqlRequest.getProject()), entry.getRealizationName(), entry.getRpcServer(),
                    entry.getException());
            setRPCStats(rpcMetricsEvent, //
                    entry.getCallTimeMs(), entry.getSkippedRows(), entry.getScannedRows(), entry.getReturnedRows(),
                    entry.getAggregatedRows());
            //For update rpc level related metrics
            MetricsManager.getInstance().update(rpcMetricsEvent);
        }
        for (QueryContext.CubeSegmentStatisticsResult contextEntry : sqlResponse.getCubeSegmentStatisticsList()) {
            RecordEvent queryMetricsEvent = new TimedRecordEvent(
                    KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQuery());
            setQueryWrapper(queryMetricsEvent, //
                    user, sqlRequest.getSql(), sqlResponse.isStorageCacheUsed() ? "CACHE" : contextEntry.getQueryType(),
                    norm(sqlRequest.getProject()), contextEntry.getRealization(), contextEntry.getRealizationType(),
                    sqlResponse.getThrowable());

            long totalStorageReturnCount = 0L;
            if (contextEntry.getQueryType().equalsIgnoreCase(OLAPQuery.EnumeratorTypeEnum.OLAP.name())) {
                for (Map<String, QueryContext.CubeSegmentStatistics> cubeEntry : contextEntry.getCubeSegmentStatisticsMap()
                        .values()) {
                    for (QueryContext.CubeSegmentStatistics segmentEntry : cubeEntry.values()) {
                        RecordEvent cubeSegmentMetricsEvent = new TimedRecordEvent(
                                KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryCube());

                        setCubeWrapper(cubeSegmentMetricsEvent, //
                                norm(sqlRequest.getProject()), segmentEntry.getCubeName(), segmentEntry.getSegmentName(),
                                segmentEntry.getSourceCuboidId(), segmentEntry.getTargetCuboidId(),
                                segmentEntry.getFilterMask());

                        setCubeStats(cubeSegmentMetricsEvent, //
                                segmentEntry.getCallCount(), segmentEntry.getCallTimeSum(), segmentEntry.getCallTimeMax(),
                                segmentEntry.getStorageSkippedRows(), segmentEntry.getStorageScannedRows(),
                                segmentEntry.getStorageReturnedRows(), segmentEntry.getStorageAggregatedRows(),
                                segmentEntry.isIfSuccess(), 1.0 / cubeEntry.size());

                        totalStorageReturnCount += segmentEntry.getStorageReturnedRows();
                        //For update cube segment level related query metrics
                        MetricsManager.getInstance().update(cubeSegmentMetricsEvent);
                    }
                }
            } else {
                if (!sqlResponse.getIsException()) {
                    totalStorageReturnCount = sqlResponse.getResults().size();
                }
            }
            setQueryStats(queryMetricsEvent, //
                    sqlResponse.getDuration(), sqlResponse.getResults() == null ? 0 : sqlResponse.getResults().size(),
                    totalStorageReturnCount);
            //For update query level metrics
            MetricsManager.getInstance().update(queryMetricsEvent);
        }

         */

    }

    private static String norm(String project) {
        return project.toUpperCase(Locale.ROOT);
    }

    private static void setStageWrapper(RecordEvent metricsEvent, String projectName, String realizationName,
                                        String queryId, long executionId, long jobId, long stageId, boolean isSuccess) {
        metricsEvent.put(QuerySparkStageEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QuerySparkStageEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QuerySparkStageEnum.QUERY_ID.toString(), queryId);
        metricsEvent.put(QuerySparkStageEnum.EXECUTION_ID.toString(), executionId);
        metricsEvent.put(QuerySparkStageEnum.JOB_ID.toString(), jobId);
        metricsEvent.put(QuerySparkStageEnum.STAGE_ID.toString(), stageId);
        metricsEvent.put(QuerySparkStageEnum.IF_SUCCESS.toString(), isSuccess);
    }

    private static void setStageMetrics(RecordEvent metricsEvent, long resultSize, long executorDeserializeTime, long executorDeserializeCpuTime, long executorRunTime,
                                        long executorCpuTime, long jvmGCTime, long resultSerializationTime, long memoryBytesSpilled,
                                        long diskBytesSpilled, long peakExecutionMemory) {
        metricsEvent.put(QuerySparkStageEnum.RESULT_SIZE.toString(), resultSize);
        metricsEvent.put(QuerySparkStageEnum.EXECUTOR_DESERIALIZE_TIME.toString(), executorDeserializeTime);
        metricsEvent.put(QuerySparkStageEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), executorDeserializeCpuTime);
        metricsEvent.put(QuerySparkStageEnum.EXECUTOR_RUN_TIME.toString(), executorRunTime);
        metricsEvent.put(QuerySparkStageEnum.EXECUTOR_CPU_TIME.toString(), executorCpuTime);
        metricsEvent.put(QuerySparkStageEnum.JVM_GC_TIME.toString(), jvmGCTime);
        metricsEvent.put(QuerySparkStageEnum.RESULT_SERIALIZATION_TIME.toString(), resultSerializationTime);
        metricsEvent.put(QuerySparkStageEnum.MEMORY_BYTE_SPILLED.toString(), memoryBytesSpilled);
        metricsEvent.put(QuerySparkStageEnum.DISK_BYTES_SPILLED.toString(), diskBytesSpilled);
        metricsEvent.put(QuerySparkStageEnum.PEAK_EXECUTION_MEMORY.toString(), peakExecutionMemory);
    }

    private static void setSparkJobWrapper(RecordEvent metricsEvent, String projectName, String realizationName,
                                           String queryId, long executionId, long jobId, boolean isSuccess) {
        metricsEvent.put(QuerySparkJobEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QuerySparkJobEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QuerySparkJobEnum.QUERY_ID.toString(), queryId);
        metricsEvent.put(QuerySparkJobEnum.EXECUTION_ID.toString(), executionId);
        metricsEvent.put(QuerySparkJobEnum.JOB_ID.toString(), jobId);
        metricsEvent.put(QuerySparkJobEnum.IF_SUCCESS.toString(), isSuccess);
    }

    private static void setSparkJobMetrics(RecordEvent metricsEvent, long resultSize, long executorDeserializeTime, long executorDeserializeCpuTime, long executorRunTime,
                                        long executorCpuTime, long jvmGCTime, long resultSerializationTime, long memoryBytesSpilled,
                                        long diskBytesSpilled, long peakExecutionMemory) {
        metricsEvent.put(QuerySparkJobEnum.RESULT_SIZE.toString(), resultSize);
        metricsEvent.put(QuerySparkJobEnum.EXECUTOR_DESERIALIZE_TIME.toString(), executorDeserializeTime);
        metricsEvent.put(QuerySparkJobEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), executorDeserializeCpuTime);
        metricsEvent.put(QuerySparkJobEnum.EXECUTOR_RUN_TIME.toString(), executorRunTime);
        metricsEvent.put(QuerySparkJobEnum.EXECUTOR_CPU_TIME.toString(), executorCpuTime);
        metricsEvent.put(QuerySparkJobEnum.JVM_GC_TIME.toString(), jvmGCTime);
        metricsEvent.put(QuerySparkJobEnum.RESULT_SERIALIZATION_TIME.toString(), resultSerializationTime);
        metricsEvent.put(QuerySparkJobEnum.MEMORY_BYTE_SPILLED.toString(), memoryBytesSpilled);
        metricsEvent.put(QuerySparkJobEnum.DISK_BYTES_SPILLED.toString(), diskBytesSpilled);
        metricsEvent.put(QuerySparkJobEnum.PEAK_EXECUTION_MEMORY.toString(), peakExecutionMemory);
    }

    private static void setSparkExecutionWrapper(RecordEvent metricsEvent, String user, String sql, String queryType,
                                                  String queryId, long executionId, String projectName, String realizationName,
                                                  String cuboidIds, Throwable throwable) {
        metricsEvent.put(QuerySparkExecutionEnum.USER.toString(), user);
        metricsEvent.put(QuerySparkExecutionEnum.ID_CODE.toString(), getSqlHashCode(sql));
        metricsEvent.put(QuerySparkExecutionEnum.SQL.toString(), sql);
        metricsEvent.put(QuerySparkExecutionEnum.TYPE.toString(), queryType);
        metricsEvent.put(QuerySparkExecutionEnum.QUERY_ID.toString(), queryId);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTION_ID.toString(), executionId);
        metricsEvent.put(QuerySparkExecutionEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QuerySparkExecutionEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QuerySparkExecutionEnum.CUBOID_IDS.toString(), cuboidIds);
        metricsEvent.put(QuerySparkExecutionEnum.EXCEPTION.toString(),
                throwable == null ? "NULL" : throwable.getClass().getName());
    }

    private static void setSparkExecutionMetrics(RecordEvent metricsEvent, long sqlDuration, long totalScanCount, long totalScanBytes,
                                                 long resultCount, long executionDuration, long resultSize, long executorDeserializeTime,
                                                 long executorDeserializeCpuTime, long executorRunTime, long executorCpuTime,
                                                 long jvmGCTime, long resultSerializationTime, long memoryBytesSpilled,
                                                 long diskBytesSpilled, long peakExecutionMemory) {
        metricsEvent.put(QuerySparkExecutionEnum.SQL_DURATION.toString(), sqlDuration);
        metricsEvent.put(QuerySparkExecutionEnum.TOTAL_SCAN_COUNT.toString(), totalScanCount);
        metricsEvent.put(QuerySparkExecutionEnum.TOTAL_SCAN_BYTES.toString(), totalScanBytes);
        metricsEvent.put(QuerySparkExecutionEnum.RESULT_COUNT.toString(), resultCount);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTION_DURATION.toString(), executionDuration);

        metricsEvent.put(QuerySparkExecutionEnum.RESULT_SIZE.toString(), resultSize);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTOR_DESERIALIZE_TIME.toString(), executorDeserializeTime);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), executorDeserializeCpuTime);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTOR_RUN_TIME.toString(), executorRunTime);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTOR_CPU_TIME.toString(), executorCpuTime);
        metricsEvent.put(QuerySparkExecutionEnum.JVM_GC_TIME.toString(), jvmGCTime);
        metricsEvent.put(QuerySparkExecutionEnum.RESULT_SERIALIZATION_TIME.toString(), resultSerializationTime);
        metricsEvent.put(QuerySparkExecutionEnum.MEMORY_BYTE_SPILLED.toString(), memoryBytesSpilled);
        metricsEvent.put(QuerySparkExecutionEnum.DISK_BYTES_SPILLED.toString(), diskBytesSpilled);
        metricsEvent.put(QuerySparkExecutionEnum.PEAK_EXECUTION_MEMORY.toString(), peakExecutionMemory);
    }


    private static void update(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        try {
            incrQueryCount(queryMetrics, sqlResponse);
            incrCacheHitCount(queryMetrics, sqlResponse);

            if (!sqlResponse.getIsException()) {
                queryMetrics.addQueryLatency(sqlResponse.getDuration());
                queryMetrics.addScanRowCount(sqlResponse.getTotalScanCount());
                queryMetrics.addResultRowCount(sqlResponse.getResults().size());
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    private static void incrQueryCount(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        if (!sqlResponse.isHitExceptionCache() && !sqlResponse.getIsException()) {
            queryMetrics.incrQuerySuccessCount();
        } else {
            queryMetrics.incrQueryFailCount();
        }
        queryMetrics.incrQueryCount();
    }

    private static void incrCacheHitCount(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        if (sqlResponse.isStorageCacheUsed()) {
            queryMetrics.addCacheHitCount(1);
        }
    }

    private static QueryMetrics getQueryMetrics(String name) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int[] intervals = config.getQueryMetricsPercentilesIntervals();

        QueryMetrics queryMetrics = metricsMap.get(name);
        if (queryMetrics != null) {
            return queryMetrics;
        }

        synchronized (QueryMetricsFacade.class) {
            queryMetrics = metricsMap.get(name);
            if (queryMetrics != null) {
                return queryMetrics;
            }

            try {
                queryMetrics = new QueryMetrics(intervals).registerWith(name);
                metricsMap.put(name, queryMetrics);
                return queryMetrics;
            } catch (MetricsException e) {
                logger.warn(name + " register error: ", e);
            }
        }
        return queryMetrics;
    }
}
