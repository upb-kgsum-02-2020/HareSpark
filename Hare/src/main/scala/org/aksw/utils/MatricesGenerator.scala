package org.aksw.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import net.sansa_stack.rdf.spark.io.NTripleReader

object MatricesGenerator {
  
//  /home/gsjunior/Downloads
//  var sourcePath = "src/main/resources/sample_triples.nt"
//  var sourcePath = "/home/gsjunior/Documentos/datasets/airports.nt"
//  var w_dest = "src/main/resources/matrices/sample_triples/w.txt"
//  var f_dest = "src/main/resources/matrices/sample_triples/f.txt"
//  var edges_triples_dest = "src/main/resources/matrices/sample_triples/edges_triples.txt"
//  var edges_resources_dest = "src/main/resources/matrices/sample_triples/edges_resources.txt"
  
  var sourcePath = ""
  var w_dest = "/matrices/w"
  var f_dest = "/matrices/f"
  var edges_triples_dest = "/matrices/edges_triples"
  var edges_resources_dest = "/matrices/edges_resources"
  var statistics_dest = "/statistics"

  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
//      .master("local[*]")
      .appName("MatrixGenerator")
      .getOrCreate()
      
    import spark.implicits._
    
    sourcePath = args(0) + sourcePath
    w_dest = args(1) + w_dest
    f_dest = args(1) + f_dest
    edges_triples_dest = args(1) + edges_triples_dest
    edges_resources_dest = args(1) + edges_resources_dest
    statistics_dest = args(1) + statistics_dest
    
    
    val sc = spark.sparkContext

    val rdd = sc.textFile(sourcePath)
    
    val t1 = System.currentTimeMillis()
    
    val sansa_triples = NTripleReader.load(spark, sourcePath)

        //Creating rdd's for nodes
    val nodes_triples_rdd = sansa_triples.map(f => f.toString() )
    
    val nodes_subject_rdd = sansa_triples.map(f => f.getSubject.toString())
    val nodes_predicate_rdd = sansa_triples.map(f => f.getPredicate.toString())
    val nodes_object_rdd = sansa_triples.map(f => f.getObject.toString())
    
    //Creating rdd's for edges
    
    val edges_subject_rdd = sansa_triples.map(f => (f.toString(),f.getSubject.toString))
    val edges_predicate_rdd = sansa_triples.map(f => (f.toString(),f.getPredicate.toString))
    val edges_object_rdd = sansa_triples.map(f => (f.toString(),f.getObject.toString))
    

//    val nodes_triples_rdd = rdd.map(x => x.replaceAll("\\s+", "-"))
//    
//    val nodes_subject_rdd = rdd.map(x => x.split("\\s+")(0))
//    val nodes_predicate_rdd = rdd.map(x => x.split("\\s+")(1))
//    val nodes_object_rdd = rdd.map(x => x.split("\\s+")(2))
//    
//    
//    
//    val edges_subject_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(0)))
//    val edges_predicate_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(1)))
//    val edges_object_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(2)))
    
    
    // Creating rdd's for nodes and edges
    val nodes_triples = nodes_triples_rdd.distinct().zipWithIndex().map(f => (f._1,f._2.toString()+"t"))
    val nodes_entities = nodes_subject_rdd.union(nodes_predicate_rdd).union(nodes_object_rdd).distinct().zipWithIndex().map(f => (f._1,f._2.toString()+"e"))
    val nodes_rdd = nodes_triples.union(nodes_entities).toDF("node","id")
                
    val edges_rdd = edges_subject_rdd
                    .union(edges_predicate_rdd)
                    .union(edges_object_rdd).toDF("src","dest")
                    
      

      var edges_df = edges_rdd.join(nodes_rdd,nodes_rdd.col("node") === edges_rdd.col("src")).withColumnRenamed("id", "id2").drop("node")
      edges_df = edges_df.join(nodes_rdd,nodes_rdd.col("node") === edges_rdd.col("dest")).drop("node").drop("src").drop("dest")
      edges_df = edges_df.withColumnRenamed("id2", "src").withColumnRenamed("id", "dest")
      
    
    var final_matrix = edges_df.rdd.map(f => (f.toSeq(0).toString(),f.toSeq(1).toString()))

     val map_edges_triples = final_matrix.groupBy(x => x._1).sortByKey(true)
     val map_edges_resources = final_matrix.groupBy(x => x._2).sortByKey(true)
     
//     map_edges_triples.repartition(1).saveAsTextFile("src/main/resources/edges_triples")
//     map_edges_resources.repartition(1).saveAsTextFile("src/main/resources/edges_resources")
     
     edges_df.show

     val count_triples = map_edges_triples.count
     val count_resources = map_edges_resources.count
     
     val parseTime = (System.currentTimeMillis() - t1) / 1000
     
     val t2 = System.currentTimeMillis()
       
     val w_rdd = map_edges_triples.map{
               x=>     
               val p = 1.0 / x._2.size.toDouble
               
               val values = x._2.toArray
               
               val me = new Array[MatrixEntry](values.size)
              for(a<- 0 to values.size-1){
                     val matrixEntry = new MatrixEntry(values(a)._1.replaceAll("t", "").toLong,values(a)._2.replaceAll("e", "").toLong,p)
                     me(a) = matrixEntry
               }
               me
         }.flatMap(f => f).filter(f => f != null)
         
         
     

     val f_rdd = map_edges_resources.map{
               x=>     
               val p = 1.0 / x._2.size.toDouble

               val values = x._2.toArray
               
               val me = new Array[MatrixEntry](values.size)
              for(a<- 0 to values.size-1){
                     val matrixEntry = new MatrixEntry(values(a)._2.replaceAll("e", "").toLong,values(a)._1.replaceAll("t", "").toLong,p)
                     me(a) = matrixEntry
                   }
                 
               

               me
         }.flatMap(f => f).filter(f => f != null)
               
      val w = w_rdd.map{ x => x.i + "," + x.j + "," + x.value}
      val f = f_rdd.map{ x => x.i + "," + x.j + "," + x.value}
      
      
       
           w.saveAsTextFile(w_dest)
           f.saveAsTextFile(f_dest)
      
     
      
      val matrixTime = (System.currentTimeMillis() - t2) / 1000
      
      val statistics = new ListBuffer[String]()
      statistics += "Number of Triples: " + count_triples
      statistics += "Number of Entities: " + count_resources
      statistics += "Parsing File Time: " + parseTime
      statistics += "Matrix Generation Time: " + matrixTime
      
      val rdd_statistics = sc.parallelize(statistics)
      rdd_statistics.repartition(1).saveAsTextFile(statistics_dest)
      
  }
  
}