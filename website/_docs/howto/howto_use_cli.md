---
layout: docs
title:  Use Utility CLIs
categories: howto
permalink: /docs/howto/howto_use_cli.html
---
Kylin has some client utility tools. This document will introduce the following class: KylinConfigCLI.java, CubeMetaExtractor.java, CubeMetaIngester.java, CubeMigrationCLI.java and CubeMigrationCheckCLI.java. Before using these tools, you have to switch to the KYLIN_HOME directory. 

## KylinConfigCLI.java

### Function
KylinConfigCLI.java outputs the value of Kylin properties. 

### How to use 
After the class name, you can only write one parameter, `conf_name` which is the parameter name that you want to know its value.
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.KylinConfigCLI <conf_name>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.KylinConfigCLI kylin.server.mode
{% endhighlight %}
Result:
{% highlight Groff markup %}
all
{% endhighlight %}

If you do not know the full parameter name, you can use the following command, then all parameters prefixed by this prefix will be listed:
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.KylinConfigCLI <prefix>.
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.KylinConfigCLI kylin.job.
{% endhighlight %}
Result:
{% highlight Groff markup %}
max-concurrent-jobs=10
retry=3
sampling-percentage=100
{% endhighlight %}

## CubeMetaExtractor.java

### Function
CubeMetaExtractor.java is to extract Cube related info for debugging / distributing purpose.  

### How to use
At least two parameters should be followed. 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.extractor.CubeMetaExtractor -<conf_name> <conf_value> -destDir <your_dest_dir>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.extractor.CubeMetaExtractor -cube kylin_sales_cube -destDir /tmp/kylin_sales_cube
{% endhighlight %}
Result:
After the command is executed, the cube, project or hybrid you want to extract will be dumped in the specified path.

All supported parameters are listed below:  

| Parameter                                             | Description                                                                                         |
| ----------------------------------------------------- | :-------------------------------------------------------------------------------------------------- |
| allProjects                                           | Specify realizations in all projects to extract                                                     |
| compress <compress>                                   | Specify whether to compress the output with zip. Default true.                                      | 
| cube <cube>                                           | Specify which Cube to extract                                                                       |
| destDir <destDir>                                     | (Required) Specify the dest dir to save the related information                                     |
| hybrid <hybrid>                                       | Specify which hybrid to extract                                                                     |
| includeJobs <includeJobs>                             | Set this to true if want to extract job info/outputs too. Default false                             |
| includeSegmentDetails <includeSegmentDetails>         | Set this to true if want to extract segment details too, such as dict, tablesnapshot. Default false |
| includeSegments <includeSegments>                     | Set this to true if want extract the segments info. Default true                                    |
| onlyOutput <onlyOutput>                               | When include jobs, only extract output of job. Default true                                         |
| packagetype <packagetype>                             | Specify the package type                                                                            |
| project <project>                                     | Which project to extract                                                    |
                             |

## CubeMetaIngester.java

### Function
CubeMetaIngester.java is to ingest the extracted cube meta data into another metadata store. It only supports ingest cube now. 

### How to use
At least two parameters should be specified. Please make sure the cube you want to ingest does not exist in the target project. 

Note: The zip file must contain only one directory after it has been decompressed.

{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMetaIngester -project <target_project> -srcPath <your_src_dir>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMetaIngester -project querytest -srcPath /tmp/newconfigdir1/cubes.zip
{% endhighlight %}
Result:
After the command is successfully executed, the cube you want to ingest will exist in the srcPath.

All supported parameters are listed below:

| Parameter                         | Description                                                                                                                                                                                        |
| --------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| forceIngest <forceIngest>         | Skip the target Cube, model and table check and ingest by force. Use in caution because it might break existing cubes! Suggest to backup metadata store first. Default false.                      |
| overwriteTables <overwriteTables> | If table meta conflicts, overwrite the one in metadata store with the one in srcPath. Use in caution because it might break existing cubes! Suggest to backup metadata store first. Default false. |
| createProjectIdNotExists <createProjectIdNotExists>| If the specified project is not exists, kylin will create it.                                                                                                                     |
| project <project>                 | (Required) Specify the target project for the new cubes.                                                                                                                                           |
| srcPath <srcPath>                 | (Required) Specify the path to the extracted Cube metadata zip file.                                                                                                                               |

## CubeMigrationCheckCLI.java

### Function
CubeMigrationCheckCLI.java serves for the purpose of checking the "KYLIN_HOST" property to be consistent with the dst's MetadataUrlPrefix for all of Cube segments' corresponding HTables after migrating a Cube. CubeMigrationCheckCLI.java will be called in CubeMigrationCLI.java and is usually not used separately. 

### How to use
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMigrationCheckCLI -fix <conf_value> -dstCfgUri <dstCfgUri_value> -cube <cube_name>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMigrationCheckCLI -fix true -dstCfgUri kylin-prod:7070 -cube querycube
{% endhighlight %}
All supported parameters are listed below:

| Parameter           | Description                                                                   |
| ------------------- | :---------------------------------------------------------------------------- |
| fix                 | Fix the inconsistent Cube segments' HOST, default false                       |
| dstCfgUri           | The KylinConfig of the Cube’s new home                                       |
| cube                | The cube name.                                                     |