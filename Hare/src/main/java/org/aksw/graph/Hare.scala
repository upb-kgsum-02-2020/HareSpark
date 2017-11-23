package org.aksw.graph


import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge

object Hare {
  
  
   def main(args: Array[String]): Unit = {
     
     
    val conf = new SparkConf
    conf.setMaster("local[*]")
    conf.setAppName("HareImpl")
    
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("src/main/resources/hare.nt")
 
    val graph = GraphUtils.createBiPartieGraph(rdd)
    
    val nodecount = graph.vertices.count.toDouble
    
    val pr = graph.pageRank(0.0001)
    
    
    val ordering = new Ordering[Tuple2[VertexId,Double]] {
    def compare(x:Tuple2[VertexId, Double], y:Tuple2[VertexId, Double]):Int =
    x._2.compareTo(y._2) }
    
    
    val top10 = pr.vertices.top(pr.vertices.count.toInt)(ordering)
    
    println("Top 10: ")
    top10.foreach(x => println(x._1 + ": " + (x._2 / nodecount)))
    
    val edge_count = graph.numEdges.toDouble

     var sum = top10.map(_._2).sum / nodecount
     println("soma: " + sum)
    
     
   }
  
}