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

package org.apache.kylin.rest.job;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.job.exception.SchedulerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.kylin.common.util.LocalFileMetadataTestCase.cleanAfterClass;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StorageCleanupJobTest {

    private KylinConfig kylinConfig;

    @Before
    public void setup() throws SchedulerException {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() {
        cleanAfterClass();
    }

    @Test
    public void test() throws Exception {
        FileSystem mockFs = mock(FileSystem.class);
        Path basePath = new Path(kylinConfig.getHdfsWorkingDirectory());
        prepareHDFSFiles(basePath, mockFs);

        StorageCleanupJob job = new StorageCleanupJob(kylinConfig, mockFs);
        job.execute(new String[] { "--delete", "true" });

        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        verify(mockFs, times(5)).delete(pathCaptor.capture(), eq(true));
        ArrayList<Path> expected = Lists.newArrayList(
                // Verify clean job temp directory
                new Path(basePath + "/default/job_tmp"),

                //Verify clean dropped cube
                new Path(basePath + "/default/parquet/dropped_cube"),

                //Verify clean deleted project
                new Path(basePath + "/deleted_project"),

                // Verify clean none used segments
                new Path(basePath + "/default/parquet/ci_left_join_cube/20120101000000_20130101000000_VRC"),
                new Path(basePath + "/default/parquet/ci_left_join_cube/20130101000000_20140101000000_PCN")
        );
        assertEquals(expected, pathCaptor.getAllValues());
    }

    private void prepareHDFSFiles(Path basePath, FileSystem mockFs) throws IOException {

        FileStatus[] segmentStatuses = new FileStatus[2];
        FileStatus segment1 = mock(FileStatus.class);
        FileStatus segment2 = mock(FileStatus.class);

        FileStatus[] cubeStatuses = new FileStatus[3];
        FileStatus cube1 = mock(FileStatus.class);
        FileStatus cube2 = mock(FileStatus.class);
        FileStatus cube3 = mock(FileStatus.class);

        FileStatus[] projectStatuses = new FileStatus[2];
        FileStatus project1 = mock(FileStatus.class);
        FileStatus project2 = mock(FileStatus.class);

        // Remove job temp directory
        Path jobTmpPath = new Path(basePath + "/default/job_tmp");
        when(mockFs.exists(jobTmpPath)).thenReturn(true);
        when(mockFs.delete(jobTmpPath, true)).thenReturn(true);

        // remove every segment working dir from deletion list, so this exclude.
        when(segment1.getPath()).thenReturn(new Path(basePath + "/default/parquet/ci_left_join_cube/20120101000000_20130101000000_VRC"));
        when(segment2.getPath()).thenReturn(new Path(basePath + "/default/parquet/ci_left_join_cube/20130101000000_20140101000000_PCN"));
        segmentStatuses[0] = segment1;
        segmentStatuses[1] = segment2;

        Path cubePath1 = new Path(basePath + "/default/parquet/ci_left_join_cube");
        Path cubePath2 = new Path(basePath + "/default/parquet/ci_inner_join_cube");
        Path cubePath3 = new Path(basePath + "/default/parquet/dropped_cube");
        when(mockFs.exists(cubePath1)).thenReturn(true);
        when(mockFs.exists(cubePath2)).thenReturn(false);
        when(mockFs.exists(cubePath3)).thenReturn(true);

        when(cube1.getPath()).thenReturn(cubePath1);
        when(cube2.getPath()).thenReturn(cubePath2);
        when(cube3.getPath()).thenReturn(cubePath3);
        cubeStatuses[0] = cube1;
        cubeStatuses[1] = cube2;
        cubeStatuses[2] = cube3;

        when(project1.getPath()).thenReturn(new Path(basePath + "/default"));
        when(project2.getPath()).thenReturn(new Path(basePath + "/deleted_project"));
        projectStatuses[0] = project1;
        projectStatuses[1] = project2;

        Path defaultProjectParquetPath = new Path(basePath + "/default/parquet");
        Path deletedProjectParquetPath = new Path(basePath + "/deleted_project/parquet");
        when(mockFs.exists(defaultProjectParquetPath)).thenReturn(true);
        when(mockFs.exists(deletedProjectParquetPath)).thenReturn(true);

        when(mockFs.exists(basePath)).thenReturn(true);
        when(mockFs.listStatus(new Path(basePath + "/default/parquet/ci_left_join_cube"))).thenReturn(segmentStatuses);
        when(mockFs.listStatus(defaultProjectParquetPath)).thenReturn(cubeStatuses);
        when(mockFs.listStatus(basePath)).thenReturn(projectStatuses);
    }
}
