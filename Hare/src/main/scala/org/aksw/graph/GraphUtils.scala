package org.aksw.graph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.util.MurmurHash


object GraphUtils {
  
  def createBiPartieGraph(ntriples_rdd: RDD[String]): Graph[String,String] ={
    
    val vertices_triple = ntriples_rdd.map{
          x=>
            var n = x.replaceAll("\\>","").replaceAll("\\<", "")
            n.substring(0 , n.length -2)
        }
        
        val vertices_subject = ntriples_rdd.map{
          x=>
            var n = x.replaceAll("\\>","").replaceAll("\\<", "")
            var s = n.substring(0 , n.length -2)
            s.split("\\s+")(0)
        }
        
        val vertices_predicate = ntriples_rdd.map{
          x=>
            var n = x.replaceAll("\\>","").replaceAll("\\<", "")
            var p = n.substring(0 , n.length -2)
            p.split("\\s+")(1)
        }
        
         val vertices_object = ntriples_rdd.map{
          x=>
            var n = x.replaceAll("\\>","").replaceAll("\\<", "")
            var o = n.substring(0 , n.length -2)
            o.split("\\s+")(1)
        }
     
     <!-- EDGES RDD -->
    
        val edges_subject = ntriples_rdd.map{
          x=>
            var n = x.replaceAll("\\>","").replaceAll("\\<", "")
            var s = n.split("\\s+")(0)
            
            (n.substring(0 , n.length -2), s)
        }
        
        val edges_predicate = ntriples_rdd.map{
          x=>
            var n = x.replaceAll("\\>","").replaceAll("\\<", "")
            var p = n.split("\\s+")(1)
            
            (n.substring(0 , n.length -2), p)
        }
        
         val edges_object = ntriples_rdd.map{
          x=>
            var n = x.replaceAll("\\>","").replaceAll("\\<", "")
            var o = n.split("\\s+")(2)
            
            (n.substring(0 , n.length -2) , o)
        }
     
     var edges = edges_subject.union(edges_predicate).union(edges_object)
     var vertices = vertices_triple.union(vertices_subject).union(vertices_predicate).union(vertices_object)
     
     edges = edges.distinct
     vertices = vertices.distinct
     
     val vex_rdd = vertices.map{
       x=> (x.hashCode.toLong,x)
     }
     
     val edg_rdd = vertices.map{
       x => Edge(x(0).hashCode.toLong,x(1).hashCode.toLong,x)
     }
     
     
     edges = edges.distinct()
     vertices = vertices.distinct()
     
     
     val graph = Graph(vex_rdd,edg_rdd)
     
     return graph
    
  }
  
  def main(args: Array[String]): Unit = {
    
    val rdf = "/home/gsjunior/test_data/foad_triple.nt"
    
    
    val conf = new SparkConf
    conf.setAppName("HareTest")
    conf.setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ntriples_rdd = sc.textFile(rdf)

    
    
//    val
    
   
    
     
//    val ordering = new Ordering[Tuple2[VertexId,Double]] {
//    def compare(x:Tuple2[VertexId, Double], y:Tuple2[VertexId, Double]):Int =
//    x._2.compareTo(y._2) }
//    val top10 = pr.vertices.top(pr.vertices.count.toInt)(ordering)
//    
//    
//    
//    val edge_count = graph.numEdges.toDouble
//    
//    println("Top 10: ")
//    
//   var sum = top10.map(_._2).sum
//   
//  
//   
//   println("soma total: " + sum)

  }
  

  
}