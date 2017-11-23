package org.aksw.computations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import scala.collection.immutable.ListMap
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.aksw.utils.MatrixUtils
import org.apache.spark.mllib.linalg.Matrices

object Hare {
  
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    conf.setAppName("HareImpl")
    conf.setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
//    val hadoopConf = new org.apache.hadoop.conf.Configuration()
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path("src/main/resources/result.txt"), true) } catch { case _ : Throwable => { } }
    
    val rdd = sc.textFile("src/main/resources/sample_triples.nt")
    
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
     val map_edges_triples = final_matrix.groupBy(x => x._1).sortByKey(true)
     val map_edges_entities = final_matrix.groupBy(x => x._2).sortByKey(true)
                       
     
     val total_nodes_broadcast = sc.broadcast(nodes_rdd.collect().length)
     val total_entities_broadcast = sc.broadcast(entities_rdd.collect().length)
     val total_triples_broadcast = sc.broadcast(triples_rdd.collect().length)
     
     
     val e = ListMap(entities_rdd.map{ s => s.swap }.collectAsMap().toSeq.sortBy(_._1):_*)
     val t = ListMap(triples_rdd.map{ c => c.swap }.collectAsMap().toSeq.sortBy(_._1):_*)
     
     val entities_map = sc.broadcast(e)
     val triples_map = sc.broadcast(t)
     

       
       val w_rdd = map_edges_triples.map{
               x=>     
               val p = 1.0 / x._2.size.toDouble
               val row = new Array[Double](entities_map.value.size)
               val values = x._2.toArray
               var cont = 0
               
               for ((k,v) <- entities_map.value){
                 
                   if(values.contains(x._1,k)){
                       row(cont) = p
                     }else{
                       row(cont) = 0
                     
                     }
                   cont = cont+1
               }

                Vectors.dense(row)
           }
     
     
     val f_rdd = map_edges_entities.map{
               x=> 
                 val p = 1.0 / x._2.size.toDouble
                 
                 val row = new Array[Double](triples_map.value.size)
                 val values = x._2.toArray 
                 var cont = 0
                 
                   for ((k,v) <- triples_map.value){
                 
                       if(values.contains(k,x._1)){
                           row(cont) = p
                         }else{
                           row(cont) = 0
                         
                         }
                       cont = cont+1
                   }
                Vectors.dense(row)
               }
     
     val w = new RowMatrix(w_rdd)
     val f = new RowMatrix(f_rdd)
     

     val f_array = f_rdd.map(_.toArray).take(f_rdd.count.toInt)
     val w_array = w_rdd.map(_.toArray).take(w_rdd.count.toInt)
     
     val f_local = Matrices.dense(f.numRows().toInt, f.numCols().toInt, MatrixUtils.transpose(f_array).flatten)
     val w_local = Matrices.dense(w.numRows().toInt, w.numCols().toInt, MatrixUtils.transpose(w_array).flatten)
     
     
     val p_t = w.multiply(f_local)
     val p_n = f.multiply(w_local)
     
     val s_i = f.numCols().toDouble / (w.numCols().toDouble * (f.numCols().toDouble + w.numCols().toDouble))
     
     val s_n_array = new Array[Double](entities_map.value.size)
     
     for( a <-0 to s_n_array.size - 1){
       s_n_array(a) = s_i
     }
     
     val s_n_local = Matrices.dense(s_n_array.size, 1 , s_n_array)
     
          
     val s_n_rdd = sc.parallelize(Array(Vectors.dense(s_n_array)))
     val s_n = MatrixUtils.transposeRowMatrix(new RowMatrix(s_n_rdd))
     
     
     println(s_n_local.toString(Int.MaxValue, Int.MaxValue))
     
     val s_t = MatrixUtils.transposeRowMatrix(f).multiply(s_n_local)
     
     val i_array = new Array[Double](entities_map.value.size)
     
     for( a <-0 to s_n_array.size - 1){
       i_array(a) = 1.0
     }
     
     val matrix_i = Matrices.dense(1, entities_map.value.size, i_array)
   
     val df = 0.85
     
     var s_n_final = new RowMatrix(MatrixUtils.multiplyMatrixByNumber(MatrixUtils.transposeRowMatrix(p_n), df)).multiply(s_n_local)
     
     
//     s_t.rows.repartition(1).saveAsTextFile("src/main/resources/s_t.txt")
//     s_n.rows.repartition(1).saveAsTextFile("src/main/resources/s_n.txt")
//     s_n_final.rows.repartition(1).saveAsTextFile("src/main/resources/s_n_final.txt")

  }
  

  
  
}