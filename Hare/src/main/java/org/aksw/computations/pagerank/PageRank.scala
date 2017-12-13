package org.aksw.computations.pagerank

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

object PageRank {
  
  
  def main(args: Array[String]): Unit = {
       val spark = SparkSession
      .builder()
      .appName("MatrixGenerator")
      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._
    
    val nodes_df = spark.sqlContext.read.json("src/main/resources/nodes")
    val edges_df = spark.sqlContext.read.json("src/main/resources/edges")
    
    
    
    val nodes = nodes_df.rdd.map{
         x => (x.toSeq(0).toString().toLong,x.toSeq(1).toString())
       }
       
    val node_count = nodes.count().toDouble
       
    val edges = edges_df.rdd.map{
         x => Edge(x.toSeq(0).toString().toLong,x.toSeq(1).toString().toLong,"")
       }
    
    val t1 = System.currentTimeMillis()
    val graph = Graph(nodes,edges)
    
    val rank = graph.pageRank(0.001,0.85)
    
    val ordering = new Ordering[Tuple2[Long,Double]] {
        def compare(x:Tuple2[Long, Double], y:Tuple2[Long, Double]):Int =
        x._2.compareTo(y._2) }
    
    val top10 = rank.vertices.top(10)(ordering)
    
    val final_rank = rank.vertices.map(x => (x._1,x._2 / node_count))
    println( "Total Time: " + (System.currentTimeMillis() - t1) /1000)
    
    
    
  }
  
}