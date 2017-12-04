package org.aksw.computations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import scala.collection.immutable.ListMap
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.aksw.utils.MatrixUtils
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object Hare2 {
  
  
  val df = 0.85
  
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._
    
    
    val sc = spark.sparkContext
    
    
    
//    val hadoopConf = new org.apache.hadoop.conf.Configuration()
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path("src/main/resources/result.txt"), true) } catch { case _ : Throwable => { } }
    
    val rdd = sc.textFile("src/main/resources/sample_triples.nt")
//    val rdd = sc.textFile("/home/gsjunior/Downloads/airports.nt")
    
    //Creating rdd's for nodes
    val nodes_triples_rdd = rdd.map(x => x.replaceAll("\\s+", "-"))
    
    val nodes_subject_rdd = rdd.map(x => x.split("\\s+")(0))
    val nodes_predicate_rdd = rdd.map(x => x.split("\\s+")(1))
    val nodes_object_rdd = rdd.map(x => x.split("\\s+")(2))
    
    
    //Creating rdd's for edges
    val edges_subject_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(0)))
    val edges_predicate_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(1)))
    val edges_object_rdd = rdd.map(x => (x.replaceAll("\\s+", "-"),x.split("\\s+")(2)))
    
    
    // Creating rdd's for nodes and edges
    val nodes_rdd = nodes_triples_rdd.union(nodes_subject_rdd)
                    .union(nodes_predicate_rdd)
                    .union(nodes_object_rdd)
                    .distinct().zipWithIndex().toDF("node","id")
    
                    
    val edges_rdd = edges_subject_rdd
                    .union(edges_predicate_rdd)
                    .union(edges_object_rdd).toDF("src","dest")
                    
    var edges_df = edges_rdd.join(nodes_rdd,nodes_rdd.col("node") === edges_rdd.col("src")).withColumnRenamed("id", "id2").drop("node")
    edges_df = edges_df.join(nodes_rdd,nodes_rdd.col("node") === edges_rdd.col("dest")).drop("node").drop("src").drop("dest")
    edges_df = edges_df.withColumnRenamed("id2", "src").withColumnRenamed("id", "dest")
    
    var final_matrix = edges_df.rdd.map(f => (f.toSeq(0).toString(),f.toSeq(1).toString()))
                
        
     
     val triples_rdd = nodes_rdd.rdd.map{ x=>
       if (x.toSeq(0).toString().split("-").size == 3 )
       {(x.toSeq(0).toString(),x.toSeq(1).toString())} else null }.filter(x=> x!=null)
       
     val entities_rdd = nodes_rdd.rdd.map{ x=>
       if (x.toSeq(0).toString().split("-").size == 1 )
       {(x.toSeq(0).toString(),x.toSeq(1).toString()) } else null }.filter(x=> x!=null)

                    
     
     val map_edges_triples = final_matrix.groupBy(x => x._1).sortByKey(true).zipWithIndex()
     val map_edges_resources = final_matrix.groupBy(x => x._2).sortByKey(true).zipWithIndex()
     

     val count_triples = map_edges_triples.count
     val count_resources = map_edges_resources.count
     
       val w_rdd = map_edges_triples.map{
               x=>     
               val p = 1.0 / x._1._2.size.toDouble

               val values = x._1._2.toArray
               
               val me = new Array[MatrixEntry](count_resources.toInt)
               for(a <- 0 to values.size-1){
                 val matrixEntry = new MatrixEntry(values(a)._1.toLong,values(a)._2.toLong,p)
                 me(a) = matrixEntry
               }

               me
         }.flatMap(f => f).filter(f => f != null)
     

     val f_rdd = map_edges_resources.map{
               x=> 
                 val p = 1.0 / x._1._2.size.toDouble
                 
                 val me = new Array[MatrixEntry](count_triples.toInt)
                 val values = x._1._2.toArray 
                 var cont = 0
                 
                  for(a<- 0 to values.size-1){
                     val matrixEntry = new MatrixEntry(values(a)._1.toLong,values(a)._2.toLong,p)
                     me(a) = matrixEntry
                   }
                 
                me
               }.flatMap(f => f).filter(f => f != null)
               
//      w_rdd.repartition(1).saveAsTextFile("src/main/resources/w.txt")
//      f_rdd.repartition(1).saveAsTextFile("src/main/resources/f.txt")
               
      val w = new CoordinateMatrix(w_rdd)
      val f = new CoordinateMatrix(f_rdd)
               
      
               
      val p_t = MatrixUtils.coordinateMatrixMultiply(w, f)      
      p_t.toRowMatrix().rows.repartition(1).saveAsTextFile("src/main/resources/p_t.txt")
      
//      val p_n = MatrixUtils.coordinateMatrixMultiply(f, w)
//      
//      val s_n_v = count_resources
//      
//      val s_i = f.numCols().toDouble / (w.numCols().toDouble * (f.numCols().toDouble + w.numCols().toDouble))
//      
//      var s_n_final = new CoordinateMatrix(map_edges_resources.map{ x=> 
//        new MatrixEntry(x._2,1,s_i)})
//               
//      val matrix_i = new CoordinateMatrix(map_edges_resources.map{ x=> 
//        new MatrixEntry(x._2,1,1)})
//      
//      var s_t_final = s_n_final
//      
//      var s_n_previous = s_n_final
//      
//      val epsilon = 1e-3
//      var distance = 1.toDouble
//      
//      
//      while( distance > epsilon){
//        s_n_previous = s_n_final
//        
//          s_n_final = MatrixUtils.coordinateMatrixSum(
//          MatrixUtils.coordinateMatrixMultiply(MatrixUtils.multiplyMatrixByNumber(p_n, df).transpose(),s_n_previous),
//          MatrixUtils.divideMatrixByNumber(MatrixUtils.multiplyMatrixByNumber(matrix_i, 1-df),s_n_v.toDouble))
//          
//          s_t_final = MatrixUtils.coordinateMatrixMultiply(f.transpose(), s_n_previous)
//          
//          val v1 = s_n_final.transpose().toRowMatrix().rows.collect()(0)
//          val v2 = s_n_previous.transpose().toRowMatrix().rows.collect()(0)
//      
//          distance = Vectors.sqdist(v1, v2)
//        
//      }
//      
//      s_n_final.toRowMatrix().rows.repartition(1).saveAsTextFile("src/main/resources/s_n.txt")
//      s_t_final.toRowMatrix().rows.repartition(1).saveAsTextFile("src/main/resources/s_t.txt")
      

  }
  

  
  
}