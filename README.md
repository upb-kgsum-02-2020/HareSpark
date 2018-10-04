# HareSpark

=================================================================================================
## First Steps

To generate the matrices, we use SANSA-RDF (https://github.com/SANSA-Stack/SANSA-RDF). This depencency will be included in the jar file with:

```
mvn clean install
```

You can use the ```class org.aksw.utils.MatricesGenerator``` to generate the matrices.
After building the jar file, deploy into your Spark Cluster and run as follows:

```
spark-submit -master "master" --driver-memory "XXg" --driver-cores "XXg" --executor-memory "XXg" --executor-cores "XXg" --class
org.aksw.utils.MatricesGenerator Hare.jar "path-to-nt-file" "path-where-matrices-will-be-saved"
```


## Running Hare and Pagerank
The classes ```org.aksw.computations.hare.Hare``` and ```org.aksw.computations.pagerank.PageRank``` are used to run Hare and PageRank.
They can be run as follows:

```
spark-submit -master "master" --driver-memory "XXg" --driver-cores "XXg" --executor-memory "XXg" --executor-cores "XXg" --class
org.aksw.computations.hare.Hare Hare.jar "path-where-matrices-were-saved" 
```
All the results and statistics about Hare and PageRank will be saved under the Matrices Folder.

## Spark Shuffle Problem

In traditional MapReduce frameworks, the shuffle phase is often  overshadowed  by  the  Map  and  Reduce  phases.   In fact,  shuffling is commonly integrated as part of the Re-
duce phase, even though it really has little to do with the semantics of the data.  However, shuffling data in a many-to-many  fashion  across  the  network  is  non-trivial.   The
entire working set, which is usually a large fraction of the input data, must be transferred across the network.  This places significant burden on the OS on both the source and the destination by requiring many file and network I/Os.
To achieve  high performance,  distributed coordination is important for load balancing purposes.  Especially under big data workloads, this is a known problem.

Since Hare requires less computations (multiplications and additions) than PageRank, still the shuffle problem may occur in very large data sets,
depending of the available resources(memory) in your cluster.

The video below presents a summary about shuffling:

[![Watch the video](https://images.drivereasy.com/wp-content/uploads/2017/07/img_596dda8d77553.png)](https://d3c33hcgiwev3.cloudfront.net/sVTjU5BgEeeK6hKK9RrqGg.processed/full/360p/index.webm?Expires=1538784000&Signature=eB4kwQqqAfDV4klU8EGFxns6IRBHFYgv8yYPikQzAmKu4VkSVxV1015r5wptFGO9i7aLEot9qVi7jUOYDWvYdWvJs4k79ZKDHK16k2KJrKiNeUjC4N8TVbZ2jLyTj4Q-0CaD1KoNhGNVc764XdXKD-tHdZVPo-MaxOOEgpsEd~Y_&Key-Pair-Id=APKAJLTNE6QMUY6HBC5A)


In summary, memory will spill when the size of the RDD partitions at the end of the stage exceed the amount of memory available for the shuffle buffer.

You can:
 - Manually repartition() your prior stage so that you have smaller partitions from input.
 - Increase the shuffle buffer by increasing the memory in your executor processes (spark.executor.memory)
 - Increase the shuffle buffer by increasing the fraction of executor memory allocated to it (spark.shuffle.memoryFraction) from the default of 0.2. You need to give back spark.storage.memoryFraction.
 - Increase the shuffle buffer per thread by reducing the ratio of worker threads (SPARK_WORKER_CORES) to executor memory
