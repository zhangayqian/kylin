package org.apache.kylin.metrics.property;

import org.apache.kylin.shaded.com.google.common.base.Strings;

public enum QuerySparkJobEnum {
    PROJECT("PROJECT"),
    REALIZATION("REALIZATION"),
    QUERY_ID("QUERY_ID"),
    EXECUTION_ID("EXECUTION_ID"),
    JOB_ID("JOB_ID"),
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

    QuerySparkJobEnum(String name) {
        this.propertyName = name;
    }

    public static QuerySparkJobEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (QuerySparkJobEnum property : QuerySparkJobEnum.values()) {
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
