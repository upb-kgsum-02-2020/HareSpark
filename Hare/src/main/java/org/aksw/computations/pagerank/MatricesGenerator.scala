package org.aksw.computations.pagerank

import scala.collection.immutable.ListMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import scala.collection.mutable.ListBuffer

object MatricesGenerator {
  
//  /home/gsjunior/Downloads
//  var sourcePath = "src/main/resources/sample_triples.nt"
//  var sourcePath = "/home/gsjunior/Documentos/datasets/airports.nt"
//  var w_dest = "src/main/resources/matrices/sample_triples/w.txt"
//  var f_dest = "src/main/resources/matrices/sample_triples/f.txt"
//  var edges_triples_dest = "src/main/resources/matrices/sample_triples/edges_triples.txt"
//  var edges_resources_dest = "src/main/resources/matrices/sample_triples/edges_resources.txt"
  
  var sourcePath = ""
  var nodes_dest = "/graph/nodes"
  var edges_dest = "/graph/edges"
  var statistics_dest = "/statistics/statistics.txt"

  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("MatrixGenerator")
      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._
    
    sourcePath = args(0) + sourcePath
    nodes_dest = args(1) + nodes_dest
    edges_dest = args(1) + edges_dest
    statistics_dest = args(1) + statistics_dest
    
    
    val sc = spark.sparkContext

    val rdd = sc.textFile(sourcePath)
    
    //Creating rdd's for nodes

    
    //Creating rdd's for nodes
    val nodes_triples_rdd = rdd.map(x => x.replaceAll("\\s+", "-"))
    
    val nodes_subject_rdd = rdd.map(x => x.split("\\s+")(0))
    val nodes_predicate_rdd = rdd.map(x => x.split("\\s+")(1))
    val nodes_object_rdd = rdd.map(x => x.split("\\s+")(2))
    
    
      //Creating rdd's for edges
    val edges_subject_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(0)))
    val edges_predicate_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(1)))
    val edges_object_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(2)))
    
    val edges_subject_inverse_rdd = rdd.map(x => (x.split("\\s+")(0),x.replaceAll("\\s+", "-")))
    val edges_predicate_inverse_rdd = rdd.map(x => (x.split("\\s+")(1),x.replaceAll("\\s+", "-")))
    val edges_object_inverse_rdd = rdd.map(x => (x.split("\\s+")(2),x.replaceAll("\\s+", "-")))
    
    
    // Creating rdd's for nodes and edges
    val nodes_rdd = nodes_triples_rdd.union(nodes_subject_rdd)
                    .union(nodes_predicate_rdd)
                    .union(nodes_object_rdd)
                    .distinct().zipWithIndex().toDF("node","id")
    
                    
    val edges_rdd = edges_subject_rdd
                    .union(edges_predicate_rdd)
                    .union(edges_object_rdd)
                    .union(edges_subject_inverse_rdd)
                    .union(edges_predicate_inverse_rdd)
                    .union(edges_object_inverse_rdd).toDF("src","dest")
                    
    var edges_df = edges_rdd.join(nodes_rdd,nodes_rdd.col("node") === edges_rdd.col("src")).withColumnRenamed("id", "id2").drop("node")
    edges_df = edges_df.join(nodes_rdd,nodes_rdd.col("node") === edges_rdd.col("dest")).drop("node").drop("src").drop("dest")
    edges_df = edges_df.withColumnRenamed("id2", "src").withColumnRenamed("id", "dest")
    
    
    edges_df.write.json("src/main/resources/edges")
    nodes_rdd.write.json("src/main/resources/nodes")
    
    
                
        
     
//     val triples_rdd = nodes_rdd.rdd.map{ x=>
//       if (x.toSeq(0).toString().split("-").size == 3 )
//       {(x.toSeq(0).toString(),x.toSeq(1).toString())} else null }.filter(x=> x!=null)
//       
//     val entities_rdd = nodes_rdd.rdd.map{ x=>
//       if (x.toSeq(0).toString().split("-").size == 1 )
//       {(x.toSeq(0).toString(),x.toSeq(1).toString()) } else null }.filter(x=> x!=null)

                    
    
//      
//      val statistics = new ListBuffer[String]()
//      statistics += "Number of Triples: " + count_triples
//      statistics += "Number of Entities: " + count_resources
//      statistics += "Parsing File Time: " + parseTime
//      statistics += "Matrix Generation Time: " + matrixTime
//      
//      val rdd_statistics = sc.parallelize(statistics)
//      rdd_statistics.repartition(1).saveAsTextFile(statistics_dest)
      
  }
  
}