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
import scala.collection.mutable.ListBuffer
import java.math.BigDecimal


object Hare3 {
  
  
  val df = 0.85
  
  var w_path = "/matrices/w.txt"
  var f_path = "/matrices/f.txt"
  var map_edges_resources_dest = "/matrices/edges_resources.txt"
  var s_n_dest = "/results/s_n.txt"
  var s_t_dest = "/results/s_t.txt"
  var statistics_dest = "/statistics/hare_statistics.txt"
  
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("HareImpl")
      .getOrCreate()
      
    import spark.implicits._
      
       w_path = args(0) + w_path
       f_path = args(0) + f_path
       map_edges_resources_dest = args(0) + map_edges_resources_dest
       s_n_dest = args(0) + s_n_dest
       s_t_dest = args(0) + s_t_dest
       statistics_dest = args(0) + statistics_dest
    
      val sc = spark.sparkContext
      
      val w_rdd = sc.textFile(w_path)
      val f_rdd = sc.textFile(f_path)
      
      val map_edges_resources = sc.textFile(map_edges_resources_dest).map(f => (f.split(",")(0).toLong,f.split(",")(1).toLong))
       
      val t1 = System.currentTimeMillis()
               
      val w = loadCoordinateMatrix(w_rdd)
      val f = loadCoordinateMatrix(f_rdd)
               
      val p_t = MatrixUtils.coordinateMatrixMultiply(w, f)      
      val p_n = MatrixUtils.coordinateMatrixMultiply(f, w)
      
      val s_n_v = f.numRows()
      
      val s_i = f.numCols().toDouble / (w.numCols().toDouble * (f.numCols().toDouble + w.numCols().toDouble))
      

      
      val t = f.entries.map(f => f.i).distinct().sortBy(f =>f,true)
      
      var s_n_final = new CoordinateMatrix(t.map{ x=> 
        new MatrixEntry(x,0,s_i)})
    
         
         
      val matrix_i = new CoordinateMatrix(t.map{ x=> 
        new MatrixEntry(x,0,1)})
      
      val matrixLoadTime = (System.currentTimeMillis() - t1) / 1000
      
      var s_t_final = s_n_final
      
      var s_n_previous = s_n_final
      
      
      val epsilon = new BigDecimal(0.0001)
      var distance = new BigDecimal(1)
      
      val t2 = System.currentTimeMillis()
      var iter = 0
      while( distance.compareTo(epsilon) == 1 ){
          s_n_previous = s_n_final
          
          s_n_final = MatrixUtils.coordinateMatrixSum(
          MatrixUtils.coordinateMatrixMultiply(MatrixUtils.multiplyMatrixByNumber(p_n, df).transpose(),s_n_previous),
          MatrixUtils.divideMatrixByNumber(MatrixUtils.multiplyMatrixByNumber(matrix_i, 1-df),s_n_v.toDouble))
         
          val v1 = s_n_final.transpose().toRowMatrix().rows.collect()(0)
          val v2 = s_n_previous.transpose().toRowMatrix().rows.collect()(0)
      
          distance = new BigDecimal(Vectors.sqdist(v1, v2))
          iter = iter+1
        
      }
    
     
      s_t_final = MatrixUtils.coordinateMatrixMultiply(f.transpose(), s_n_final)
      
      s_n_final.toRowMatrix().rows.saveAsTextFile(s_n_dest)
      s_t_final.toRowMatrix().rows.saveAsTextFile(s_t_dest)
      
      val hareTime = (System.currentTimeMillis() - t2) / 1000
      
      val statistics = new ListBuffer[String]()
      statistics += "Iterations: " + iter
      statistics += "Matrices Load Time: " + matrixLoadTime
      statistics += "Hare Computation Time: " + matrixLoadTime
      
      val rdd_statistics = sc.parallelize(statistics)
      rdd_statistics.repartition(1).saveAsTextFile(statistics_dest)
      
      println("Distance: " + distance)
      println("Iterations: " + iter)

  }
  
  
  def loadCoordinateMatrix(rdd : RDD[String]): CoordinateMatrix = {
    new CoordinateMatrix(rdd.map{ x =>
      val a = x.split(",")
      new MatrixEntry(a(0).toLong,a(1).toLong,a(2).toDouble)})
  }

  
  
}