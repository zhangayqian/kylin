package org.apache.kylin.metrics.property;

import org.apache.kylin.shaded.com.google.common.base.Strings;

public enum QuerySparkExecutionEnum {
    ID_CODE("QUERY_HASH_CODE"),
    SQL("QUERY_SQL"),
    PROJECT("PROJECT"),
    TYPE("QUERY_TYPE"),
    REALIZATION("REALIZATION"),
    CUBOID_IDS("CUBOID_IDS"),
    QUERY_ID("QUERY_ID"),
    EXECUTION_ID("EXECUTION_ID"),
    IS_SUCCESS("IS_SUCCESS"),
    USER("KUSER"),
    SPARDER_NAME("SPARDER_NAME"),
    EXCEPTION("EXCEPTION"),


    SQL_DURATION("SQL_DURATION"),
    TOTAL_SCAN_COUNT("TOTAL_SCAN_COUNT"),
    TOTAL_SCAN_BYTES("TOTAL_SCAN_BYTES"),
    RESULT_COUNT("RESULT_COUNT"),

    EXECUTION_DURATION("EXECUTION_DURATION"),
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

    QuerySparkExecutionEnum(String name) {
        this.propertyName = name;
    }

    public static QuerySparkExecutionEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (QuerySparkExecutionEnum property : QuerySparkExecutionEnum.values()) {
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
