spark-demo
==========
A simple project intended to demo spark and get developers up and running quickly

>*Note*:
>   This project uses [Gradle](http://www.gradle.org). You must install [Gradle(1.11)](http://www.gradle.org/downloads).
>   If you would rather not install Gradle locally you can use the [Gradle Wrapper](http://www.gradle.org/docs/current/userguide/gradle_wrapper.html) by replacing all refernces to ```gradle``` with ```gradlew```.

How To Build:
-------------
1. Execute ```gradle build```
2. Find the artifact jars in './build/libs/'

Intellij Project Setup:
-----------------------
1. Execute ```gradle idea```
2. Open project folder in Intellij or open the generated .ipr file

>*Note*:
>   If you have any issues in Intellij a good first troubleshooting step is to execute ```gradle cleanIdea idea```

Eclipse Project Setup:
----------------------
1. Execute ```gradle eclipse```
2. Open the project folder in Eclipse

>*Note*:
>   If you have any issues in Eclipse a good first troubleshooting step is to execute ```gradle cleanEclipse eclipse```

Key Spark Links:
----------------
- [Downloads](http://spark.apache.org/downloads.html)
- [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
- [Standalone Guide](http://spark.apache.org/docs/latest/spark-standalone.html)


Using The Project:
------------------

>*Note*:
>  This guide has only been tested on Mac OS X and may assume tools that are specific to it. 
>  If working in another OS substitutes may need to be used but should be available.

### Step 1 - Build the Project: ###
1. Run ```gradle build```

### Step 2 - Run the Demos in Local mode: ###
The demos generally take the first argument as the Spark Master URL.
Setting this value to 'local' runs the demo in local mode.
The trailing number in the brackets '[#]' indicates the number of cores to use. (ex. 'local[2]' runs locally with 2 cores)

This project has a Gradle task called 'runSpark' that manages the runtime classpath for you.
This simplifies running spark jobs, ensures the same classpath is used in all modes, and shortens the development feedback loop.

The 'runSpark' Gradle task takes two arguments '-PsparkMain' and '-PsparkArgs':

- *-PsparkMain*: The main class to run.
- *-PsparkArgs*: The arguments to be passed to the main class. See the class for documentation and what arguments are expected.

Below are some sample commands for some simple demos:

- SparkPi: ```gradle runSpark -PsparkMain="com.cloudera.sa.SparkPi" -PsparkArgs="local[2] 100"```
- Sessionize: ```gradle runSpark -PsparkMain="com.cloudera.sa.Sessionize" -PsparkArgs="local[2]"```
- NetworkWordCount: ```gradle runSpark -PsparkMain="com.cloudera.sa.NetworkWordCount" -PsparkArgs="local[2] localhost 9999"```

>**Note:** 
>   The remaining steps are only required for running demos in "pseudo-distributed" mode and on a cluster.

### Step 3 - Install Spark: ###
1. Install Spark 1.0 using [Homebrew](http://brew.sh/): ```brew install apache-spark```
3. Add SPARK_HOME to your .bash_profile: ```export SPARK_HOME=/usr/local/Cellar/apache-spark/1.0.0/libexec```
5. Add SCALA_HOME and JAVA_HOME to your .bash_profile

>**Note:** 
>   You may also install on your own following the [Spark Documentation](http://spark.apache.org/docs/latest/#downloading)

### Step 4 - Configure & Start Spark: ###
1. The defaults should work for now. However, See [Cluster Launch Scripts](https://spark.apache.org/docs/latest/spark-standalone.html#cluster-launch-scripts) documentation for more information on configuring your pseudo cluster.
2. Start your Spark cluster: ```$SPARK_HOME/sbin/start-all.sh```
3. Validate the master & worker are running in the [Spark Master WebUI](http://localhost:8080)
4. Note the master URL on the [Spark Master WebUI](http://localhost:8080). It will be used when submitting jobs.
5. Shutdown when done: ```$SPARK_HOME/sbin/stop-all.sh```

### Step 5 - Run the Demos in Pseudo-Distributed mode: ###
Running in pseudo-distributed mode is almost exactly the same as local mode.
*Note*: Please see step 2 before continuing on.

To run in pseudo-distributed mode just replace 'local[#]' in the Spark Master URL argument with the URL from Step 4.

Below are some sample commands for each demo:

>*Note:* 
>   You will need to substitute in your Spark Master URL

- SparkPi: ```gradle runSpark -PsparkMain="com.cloudera.sa.SparkPi" -PsparkArgs="spark://example:7077 100"```
- Sessionize: ```gradle runSpark -PsparkMain="com.cloudera.sa.Sessionize" -PsparkArgs="spark://example:7077"```
- NetworkWordCount: ```gradle runSpark -PsparkMain="com.cloudera.sa.NetworkWordCount" -PsparkArgs="spark://example:7077 localhost 9999"```

### Step 6 - Run the Demos on a cluster: ###
The build creates a fat jar tagged with '-hadoop' that contains all dependencies needed to run on the cluster. The jar can be found in './build/libs/'.

TODO: Test this and fill out steps.

### Step 7 - Develop your own Demos: ###
Develop demos of your own and send a pull request!

Notable Tools & Frameworks:
---------------------------
- [Homebrew](http://brew.sh/)
- [Gradle](http://www.gradle.org/)
- [Guava](https://code.google.com/p/guava-libraries/)
- [Apache Commons Lang](http://commons.apache.org/proper/commons-lang/)
- [TypeSafe Config](https://github.com/typesafehub/config)

Todo List:
----------
- Create trait/class with generic context, smart defaults, and unified arg parsing (see spark-submit script for ref)
- Document whats demonstrated in each demo (avro, parquet, kryo, etc) and usage
- Add module level readme and docs
- Tune logging output configuration (Redirect verbose logs into a rolling file)
- Speed up HadoopJar task (and runSpark will follow)
