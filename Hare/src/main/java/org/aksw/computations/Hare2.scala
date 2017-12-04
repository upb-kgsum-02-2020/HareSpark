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

object Hare2 {
  
  
  val df = 0.85
  
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    conf.setAppName("HareImpl")
    conf.setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
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
    val edges_subject_rdd = rdd.map(x => Array(x.replaceAll("\\s+", "-"), x.split("\\s+")(0)))
    val edges_predicate_rdd = rdd.map(x => Array(x.replaceAll("\\s+", "-"), x.split("\\s+")(1)))
    val edges_object_rdd = rdd.map(x => Array(x.replaceAll("\\s+", "-"), x.split("\\s+")(2)))
    
    
    // Creating rdd's for nodes and edges
    val nodes_rdd = nodes_triples_rdd.union(nodes_subject_rdd)
                    .union(nodes_predicate_rdd)
                    .union(nodes_object_rdd)
                    .distinct().zipWithIndex()
                    
    val edges_rdd = edges_subject_rdd
                    .union(edges_predicate_rdd)
                    .union(edges_object_rdd)
                    
     
     val map_nodes = nodes_rdd.collectAsMap()
     val map_nodes_broadcast = sc.broadcast(map_nodes)
     
     
     
     val triples_rdd = nodes_rdd.map{ x=>
       if (x._1.split("-").size == 3 )
       {x} else null }.filter(x=> x!=null)
       
     val entities_rdd = nodes_rdd.map{ x=>
       if (x._1.split("-").size == 1 )
       {x} else null }.filter(x=> x!=null)
     
                    
     val final_matrix = edges_rdd.map{x => (map_nodes_broadcast.value(x(0)),map_nodes_broadcast.value(x(1))) }
     val map_edges_triples = final_matrix.groupBy(x => x._1).sortByKey(true).zipWithIndex()
     val map_edges_entities = final_matrix.groupBy(x => x._2).sortByKey(true).zipWithIndex()
     
     
     val e = ListMap(entities_rdd.map{ s => s.swap }.collectAsMap().toSeq.sortBy(_._1):_*)
     val t = ListMap(triples_rdd.map{ c => c.swap }.collectAsMap().toSeq.sortBy(_._1):_*)
     
     val entities_map = sc.broadcast(e)
     val triples_map = sc.broadcast(t)
     

       
       val w_rdd = map_edges_triples.map{
               x=>     
               val p = 1.0 / x._1._2.size.toDouble
//               val row = new Array[Double](entities_map.value.size)
               val values = x._1._2.toArray
               var cont = 0
               
               val me = new Array[MatrixEntry](entities_map.value.size)

               for ((k,v) <- entities_map.value){
                 
                   if(values.contains(x._1._1,k)){
                       val matrixEntry = new MatrixEntry(x._2,cont,p)
                       me(cont) = matrixEntry
                     }else{
                       val matrixEntry = new MatrixEntry(x._2,cont,0.0)
                       me(cont) = matrixEntry
                     }
                   cont = cont+1
               }

                me
     }.flatMap(f => f)
     

     val f_rdd = map_edges_entities.map{
               x=> 
                 val p = 1.0 / x._1._2.size.toDouble
                 
                 val me = new Array[MatrixEntry](triples_map.value.size)
                 val values = x._1._2.toArray 
                 var cont = 0
                 
                   for ((k,v) <- triples_map.value){
                 
                       if(values.contains(k,x._1._1)){
                         val matrixEntry = new MatrixEntry(x._2,cont,p)
                           me(cont) = matrixEntry
                         }else{
                           val matrixEntry = new MatrixEntry(x._2,cont,0)
                           me(cont) = matrixEntry
                         
                         }
                       cont = cont+1
                   }
                me
               }.flatMap(f => f)
               
      val w = new CoordinateMatrix(w_rdd)
      val f = new CoordinateMatrix(f_rdd)
               
      val p_t = MatrixUtils.coordinateMatrixMultiply(w, f)    
      val p_n = MatrixUtils.coordinateMatrixMultiply(f, w)
      
      val s_n_v = entities_map.value.size
      
      val s_i = f.numCols().toDouble / (w.numCols().toDouble * (f.numCols().toDouble + w.numCols().toDouble))
      
      var s_n_final = new CoordinateMatrix(map_edges_entities.map{ x=> 
        new MatrixEntry(x._2,1,s_i)})
               
      val matrix_i = new CoordinateMatrix(map_edges_entities.map{ x=> 
        new MatrixEntry(x._2,1,1)})
      
      var s_t_final = s_n_final
      
      var s_n_previous = s_n_final
      
      val epsilon = 1e-3
      var distance = 1.toDouble
      
      
      while( distance > epsilon){
        s_n_previous = s_n_final
        
          s_n_final = MatrixUtils.coordinateMatrixSum(
          MatrixUtils.coordinateMatrixMultiply(MatrixUtils.multiplyMatrixByNumber(p_n, df).transpose(),s_n_previous),
          MatrixUtils.divideMatrixByNumber(MatrixUtils.multiplyMatrixByNumber(matrix_i, 1-df),s_n_v.toDouble))
          
          s_t_final = MatrixUtils.coordinateMatrixMultiply(f.transpose(), s_n_final)
          
          val v1 = s_n_final.transpose().toRowMatrix().rows.collect()(0)
          val v2 = s_n_previous.transpose().toRowMatrix().rows.collect()(0)
      
          distance = Vectors.sqdist(v1, v2)
        
      }
      
      s_n_final.toRowMatrix().rows.repartition(1).saveAsTextFile("src/main/resources/s_n.txt")
      s_t_final.toRowMatrix().rows.repartition(1).saveAsTextFile("src/main/resources/s_t.txt")
      

  }
  

  
  
}