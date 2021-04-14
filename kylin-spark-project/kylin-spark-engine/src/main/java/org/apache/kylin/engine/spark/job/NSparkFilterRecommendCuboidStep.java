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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class NSparkFilterRecommendCuboidStep extends NSparkExecutable {
    protected static final Logger logger = LoggerFactory.getLogger(NSparkFilterRecommendCuboidStep.class);

    private long baseCuboid;
    private Set<Long> recommendCuboids;

    private FileSystem fs = HadoopUtil.getWorkingFileSystem();
    private Configuration conf = HadoopUtil.getCurrentConfiguration();

    public NSparkFilterRecommendCuboidStep() {
        this.setName(ExecutableConstants.STEP_NAME_FILTER_RECOMMEND_CUBOID_DATA_FOR_OPTIMIZATION);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams())).latestCopyForWrite();
        final CubeSegment optimizeSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        CubeSegment oldSegment = optimizeSegment.getCubeInstance().getOriginalSegmentToOptimize(optimizeSegment);
        Preconditions.checkNotNull(oldSegment,
                "cannot find the original segment to be optimized by " + optimizeSegment);

        baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();
        recommendCuboids = cube.getCuboidsRecommend();

        Preconditions.checkNotNull(recommendCuboids, "The recommend cuboid map could not be null");

        Path originalCuboidPath = new Path(getCuboidRootPath(oldSegment));

        try {
            for (FileStatus cuboid : fs.listStatus(originalCuboidPath)) {
                String cuboidId = cuboid.getPath().getName();
                if (cuboidId.equals(String.valueOf(baseCuboid)) || recommendCuboids.contains(Long.valueOf(cuboidId))) {
                    Path optimizeCuboidPath = new Path(getCuboidRootPath(optimizeSegment) + "/" + cuboidId);
                    FileUtil.copy(fs, cuboid.getPath(), fs, optimizeCuboidPath, false, true, conf);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to filter cuboid", e);
            return ExecuteResult.createError(e);
        }
        return new ExecuteResult();
    }

    public String getCuboidRootPath(CubeSegment segment) {
        return PathManager.getSegmentParquetStoragePath(segment.getCubeInstance(), segment.getName(),
                segment.getStorageLocationIdentifier());
    }

    @Override
    public boolean isLocalLog() {
        return false;
    }
}
