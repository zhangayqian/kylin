package org.apache.kylin.metrics.property;

import org.apache.kylin.shaded.com.google.common.base.Strings;

/**
 * Definition of Metrics dimension and measure for Spark stage
 */
public enum QuerySparkStageEnum {
    PROJECT("PROJECT"),
    QUERY_ID("QUERY_ID"),
    EXECUTION_ID("EXECUTION_ID"),
    JOB_ID("JOB_ID"),
    STAGE_ID("STAGE_ID"),
    REALIZATION("REALIZATION"),
    IF_SUCCESS("IF_SUCCESS"),

    RESULT_SIZE("RESULT_SIZE"),
    EXECUTOR_DESERIALIZE_TIME("EXECUTOR_DESERIALIZE_TIME"),
    EXECUTOR_DESERIALIZE_CPU_TIME("EXECUTOR_DESERIALIZE_CPU_TIME"),
    EXECUTOR_RUN_TIME("EXECUTOR_RUN_TIME"),
    EXECUTOR_CPU_TIME("EXECUTOR_CPU_TIME"),
    JVM_GC_TIME("JVM_GC_TIME"),
    RESULT_SERIALIZATION_TIME("RESULT_SERIALIZATION_TIME"),
    MEMORY_BYTE_SPILLED("MEMORY_BYTE_SPILLED"),
    DISK_BYTES_SPILLED("DISK_BYTES_SPILLED"),
    PEAK_EXECUTION_MEMORY("PEAK_EXECUTION_MEMORY");

    private final String propertyName;

    QuerySparkStageEnum(String name) {
        this.propertyName = name;
    }

    public static QuerySparkStageEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (QuerySparkStageEnum property : QuerySparkStageEnum.values()) {
            if (property.propertyName.equalsIgnoreCase(name)) {
                return property;
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return propertyName;
    }
}
