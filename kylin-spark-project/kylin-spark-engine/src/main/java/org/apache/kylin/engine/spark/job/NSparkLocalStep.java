package org.apache.kylin.engine.spark.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

import java.util.Map;
import java.util.Set;

public class NSparkLocalStep extends NSparkExecutable {
    private final static String[] excludedSparkConf = new String[] {"spark.executor.cores",
            "spark.executor.memoryOverhead", "spark.executor.extraJavaOptions",
            "spark.executor.instances", "spark.executor.memory", "spark.executor.extraClassPath"};

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        AbstractExecutable parent = getParentExecutable();
        if (parent instanceof DefaultChainedExecutable) {
            return ((DefaultChainedExecutable) parent).getMetadataDumpList(config);
        }
        throw new IllegalStateException("Unsupported " + this.getName() + " for non chained executable!");
    }

    @Override
    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> sparkConfigOverride = super.getSparkConfigOverride(config);
        overrideSparkConf(sparkConfigOverride);
        for (String sparkConf : excludedSparkConf) {
            if (sparkConfigOverride.containsKey(sparkConf)) {
                sparkConfigOverride.remove(sparkConf);
            }
        }
        return sparkConfigOverride;
    }

    protected void overrideSparkConf(Map<String, String> sparkConfigOverride) {
        //run resource detect job on local not cluster
        sparkConfigOverride.put("spark.master", "local");
        sparkConfigOverride.put("spark.sql.autoBroadcastJoinThreshold", "-1");
        sparkConfigOverride.put("spark.sql.adaptive.enabled", "false");
    }
}
