package org.aksw.computations.hare

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
import org.apache.spark.rdd.RDD

object Hare2 {
  
  
  val df = 0.85
  
  var w_path = "src/main/resources/matrices/sample_triples/w.txt"
  var f_path = "src/main/resources/matrices/sample_triples/f.txt"
  var map_edges_resources_dest = "src/main/resources/matrices/sample_triples/edges_resources.txt"
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("HareImpl")
      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._
    
    
      val sc = spark.sparkContext
      
      val w_rdd = sc.textFile(w_path)
      val f_rdd = sc.textFile(f_path)
      
      val map_edges_resources = sc.textFile(map_edges_resources_dest).map(f => (f.split(",")(0).toLong,f.split(",")(1).toLong))
               
               
      val w = loadCoordinateMatrix(w_rdd)
      val f = loadCoordinateMatrix(f_rdd)
               
      val p_t = MatrixUtils.coordinateMatrixMultiply(w, f)      
      val p_n = MatrixUtils.coordinateMatrixMultiply(f, w)
      
      val s_n_v = f.numRows()
      
      val s_i = f.numCols().toDouble / (w.numCols().toDouble * (f.numCols().toDouble + w.numCols().toDouble))
      
      var s_n_final = new CoordinateMatrix(map_edges_resources.map{ x=> 
        new MatrixEntry(x._2,1,s_i)})
               
      val matrix_i = new CoordinateMatrix(map_edges_resources.map{ x=> 
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
          
          
          
          val v1 = s_n_final.transpose().toRowMatrix().rows.collect()(0)
          val v2 = s_n_previous.transpose().toRowMatrix().rows.collect()(0)
      
          distance = Vectors.sqdist(v1, v2)
        
      }
      s_t_final = MatrixUtils.coordinateMatrixMultiply(f.transpose(), s_n_final)
      
      s_n_final.toRowMatrix().rows.repartition(1).saveAsTextFile("src/main/resources/s_n.txt")
      s_t_final.toRowMatrix().rows.repartition(1).saveAsTextFile("src/main/resources/s_t.txt")
      

  }
  
  
  def loadCoordinateMatrix(rdd : RDD[String]): CoordinateMatrix = {
    new CoordinateMatrix(rdd.map{ x =>
      val a = x.split(",")
      new MatrixEntry(a(0).toLong,a(1).toLong,a(2).toDouble)})
  }

  
  
}