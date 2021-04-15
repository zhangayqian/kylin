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
