package org.apache.kylin.engine.spark.job;

import org.apache.kylin.job.constant.ExecutableConstants;

public class NOptimizeRelatedStep extends NSparkLocalStep {

    public NOptimizeRelatedStep() {
    }

    public NOptimizeRelatedStep(JobStepType type) {
        switch (type) {
            case FILTER_RECOMMEND_CUBOID:
                this.setName(ExecutableConstants.STEP_NAME_FILTER_RECOMMEND_CUBOID_DATA_FOR_OPTIMIZATION);
                this.setSparkSubmitClassName(FilterRecommendCuboidJob.class.getName());
                break;
            case COPY_DICTIONARY_SNAPSHOT:
                this.setName(ExecutableConstants.STEP_NAME_COPY_DICTIONARY);
                this.setSparkSubmitClassName(CopyResourcePathJob.class.getName());
                break;
            default:
                throw new IllegalArgumentException();
        }
    }
}
