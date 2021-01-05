package org.apache.kylin.metrics;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class QuerySparkMetrics {
    private static Map<String, List<SparkJobMetrics>> sparkJobMetricsMap = Maps.newConcurrentMap();
    private static List<SparkStageMetrics> sparkStageMetricsList = new ArrayList<>();
    private static Map<Long, List<SparkStageMetrics>> sparkStageMetricsMap = Maps.newConcurrentMap();
    private static List<SparkJobMetrics> sparkJobMetricsList = new ArrayList<>();
    private static Map<String, QueryExecutionMetrics> queryExecutionMetricsMap = Maps.newConcurrentMap();

    public static void addSparkStageMetrics(int jobId, int stageId, String stageType, boolean isSuccess, long resultSize,
                                     long executorDeserializeTime, long executorDeserializeCpuTime, long executorRunTime,
                                     long executorCpuTime, long jvmGCTime, long resultSerializationTime, long memoryBytesSpilled,
                                     long diskBytesSpilled, long peakExecutionMemory) {
        SparkStageMetrics sparkStageMetrics = new SparkStageMetrics();
        sparkStageMetrics.setWrapper(jobId, stageId, stageType, isSuccess);
        sparkStageMetrics.setMetrics(resultSize, executorDeserializeTime, executorDeserializeCpuTime, executorRunTime,
                executorCpuTime, jvmGCTime, resultSerializationTime, memoryBytesSpilled, diskBytesSpilled, peakExecutionMemory);
        sparkStageMetricsList.add(sparkStageMetrics);

        sparkStageMetricsMap.put((long) jobId, sparkStageMetricsList.stream()
                .filter(stageMetrics -> stageMetrics.getJobId() == jobId)
                .collect(Collectors.toList()));
    }

    public static void addSparkJobMetrics(String queryId, long executionId, int jobId, boolean isSuccess) {
        SparkJobMetrics sparkJobMetrics = new SparkJobMetrics();
        sparkJobMetrics.setExecutionId(executionId);
        sparkJobMetrics.setJobId(jobId);
        sparkJobMetrics.setSuccess(isSuccess);
        sparkJobMetrics.setSparkStageMetricsMap(sparkStageMetricsMap.get(jobId));
        sparkJobMetricsList.add(sparkJobMetrics);

        sparkJobMetricsMap.put(queryId, sparkJobMetricsList.stream()
                .filter(jobMetrics -> jobMetrics.getExecutionId() == executionId)
                .collect(Collectors.toList()));
    }

    public static void addExecutionMetrics(String queryId, long executionId, long executionDuration, String sparderName) {
        QueryExecutionMetrics queryExecutionMetrics = new QueryExecutionMetrics();
        queryExecutionMetrics.setQueryId(queryId);
        queryExecutionMetrics.setExecutionId(executionId);
        queryExecutionMetrics.setExecutionDuration(executionDuration);
        queryExecutionMetrics.setSparderName(sparderName);
        queryExecutionMetricsMap.put(queryId, queryExecutionMetrics);
    }


    public static Map<String, QueryExecutionMetrics> getQueryExecutionMetricsMap() {
        return queryExecutionMetricsMap;
    }

    public static void setQueryRealization(String queryId, String realizationName, int realizationType) {
        QueryExecutionMetrics queryExecutionMetrics = queryExecutionMetricsMap.get(queryId);
        queryExecutionMetrics.setRealization(realizationName);
        queryExecutionMetrics.setRealizationType(realizationType);
    }

    public static class QueryExecutionMetrics implements Serializable {
        private long executionId;
        private String sparderName;
        private long executionDuration;
        private String queryId;
        private String realization;
        private int realizationType;
        private ConcurrentMap<Integer, SparkJobMetrics> sparkJobMetricsMap;

        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }

        public String getQueryId() {
            return queryId;
        }

        public long getExecutionDuration() {
            return executionDuration;
        }

        public void setExecutionDuration(long executionDuration) {
            this.executionDuration = executionDuration;
        }

        public ConcurrentMap<Integer, SparkJobMetrics> getSparkJobMetricsMap() {
            return sparkJobMetricsMap;
        }

        public long getExecutionId() {
            return executionId;
        }

        public String getSparderName() {
            return sparderName;
        }

        public void setExecutionId(long executionId) {
            this.executionId = executionId;
        }

        public void setSparderName(String sparderName) {
            this.sparderName = sparderName;
        }

        public String getRealization() {
            return realization;
        }

        public int getRealizationType() {
            return realizationType;
        }

        public void setRealization(String realization) {
            this.realization = realization;
        }

        public void setRealizationType(int realizationType) {
            this.realizationType = realizationType;
        }

        public void setSparkJobMetricsMap(ConcurrentMap<Integer, SparkJobMetrics> sparkJobMetricsMap) {
            this.sparkJobMetricsMap = sparkJobMetricsMap;
        }
    }

    public static class SparkJobMetrics implements Serializable {
        private long executionId;
        private long jobId;
        private int sparderName;
        private boolean isSuccess;
        private List<SparkStageMetrics> sparkStageMetricsList;

        public void setSparderName(int sparderName) {
            this.sparderName = sparderName;
        }

        public int getSparderName() {
            return sparderName;
        }

        public void setExecutionId(long executionId) {
            this.executionId = executionId;
        }

        public long getExecutionId() {
            return executionId;
        }

        public void setSparkStageMetricsMap(List<SparkStageMetrics> sparkStageMetricsList) {
            this.sparkStageMetricsList = sparkStageMetricsList;
        }

        public void setJobId(int jobId) {
            this.jobId = jobId;
        }

        public void setSuccess(boolean success) {
            isSuccess = success;
        }

        public boolean isSuccess() {
            return isSuccess;
        }

        public List<SparkStageMetrics> getSparkStageMetricsMap() {
            return sparkStageMetricsList;
        }

        public long getJobId() {
            return jobId;
        }
    }

    public static class SparkStageMetrics implements Serializable {
        private int jobId;
        private long stageId;
        private String stageType;
        private boolean isSuccess;
        private long resultSize;
        private long executorDeserializeTime;
        private long executorDeserializeCpuTime;
        private long executorRunTime;
        private long executorCpuTime;
        private long jvmGCTime;
        private long resultSerializationTime;
        private long memoryBytesSpilled;
        private long diskBytesSpilled;
        private long peakExecutionMemory;

        public void setWrapper(int jobId, long stageId, String stageType, boolean isSuccess) {
            this.jobId = jobId;
            this.stageId = stageId;
            this.stageType = stageType;
            this.isSuccess = isSuccess;
        }

        public void setMetrics(long resultSize, long executorDeserializeTime, long executorDeserializeCpuTime, long executorRunTime,
                               long executorCpuTime, long jvmGCTime, long resultSerializationTime, long memoryBytesSpilled,
                               long diskBytesSpilled, long peakExecutionMemory) {
            this.resultSize = resultSize;
            this.executorDeserializeTime = executorDeserializeTime;
            this.executorDeserializeCpuTime = executorDeserializeCpuTime;
            this.executorRunTime = executorRunTime;
            this.executorCpuTime = executorCpuTime;
            this.jvmGCTime = jvmGCTime;
            this.resultSerializationTime = resultSerializationTime;
            this.memoryBytesSpilled = memoryBytesSpilled;
            this.diskBytesSpilled = diskBytesSpilled;
            this.peakExecutionMemory = peakExecutionMemory;
        }

        public int getJobId() {
            return jobId;
        }

        public void setJobId(int jobId) {
            this.jobId = jobId;
        }

        public boolean isSuccess() {
            return isSuccess;
        }

        public void setSuccess(boolean success) {
            isSuccess = success;
        }

        public void setStageType(String stageType) {
            this.stageType = stageType;
        }

        public void setStageId(int stageId) {
            this.stageId = stageId;
        }

        public void setResultSize(long resultSize) {
            this.resultSize = resultSize;
        }

        public void setResultSerializationTime(long resultSerializationTime) {
            this.resultSerializationTime = resultSerializationTime;
        }

        public void setPeakExecutionMemory(long peakExecutionMemory) {
            this.peakExecutionMemory = peakExecutionMemory;
        }

        public void setMemoryBytesSpilled(long memoryBytesSpilled) {
            this.memoryBytesSpilled = memoryBytesSpilled;
        }

        public void setJvmGCTime(long jvmGCTime) {
            this.jvmGCTime = jvmGCTime;
        }

        public void setExecutorRunTime(long executorRunTime) {
            this.executorRunTime = executorRunTime;
        }

        public void setExecutorDeserializeTime(long executorDeserializeTime) {
            this.executorDeserializeTime = executorDeserializeTime;
        }

        public void setExecutorDeserializeCpuTime(long executorDeserializeCpuTime) {
            this.executorDeserializeCpuTime = executorDeserializeCpuTime;
        }

        public void setExecutorCpuTime(long executorCpuTime) {
            this.executorCpuTime = executorCpuTime;
        }

        public void setDiskBytesSpilled(long diskBytesSpilled) {
            this.diskBytesSpilled = diskBytesSpilled;
        }

        public String getStageType() {
            return stageType;
        }

        public long getResultSize() {
            return resultSize;
        }

        public long getResultSerializationTime() {
            return resultSerializationTime;
        }

        public long getPeakExecutionMemory() {
            return peakExecutionMemory;
        }

        public long getMemoryBytesSpilled() {
            return memoryBytesSpilled;
        }

        public long getJvmGCTime() {
            return jvmGCTime;
        }

        public long getExecutorRunTime() {
            return executorRunTime;
        }

        public long getExecutorDeserializeTime() {
            return executorDeserializeTime;
        }

        public long getExecutorDeserializeCpuTime() {
            return executorDeserializeCpuTime;
        }

        public long getExecutorCpuTime() {
            return executorCpuTime;
        }

        public long getDiskBytesSpilled() {
            return diskBytesSpilled;
        }

        public long getStageId() {
            return stageId;
        }
    }
}
