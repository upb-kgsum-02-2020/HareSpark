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
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.aksw.utils.DistanceUtils


object Hare {
  
  
  val df = 0.85
  
  var w_path = "/matrices/w"
  var f_path = "/matrices/f"
  var results_dest = "/results_hare"

  var statistics_dest = "/hare_statistics"
  var entities_dest = "/entites"
  
  
  def main(args: Array[String]): Unit = {
    
   
    val spark = SparkSession
      .builder()
      .appName("HareScalaSpark-" + args(0).substring(args(0).lastIndexOf("/") + 1))
//      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._
      
       w_path = args(0) + w_path
       f_path = args(0) + f_path
       results_dest = args(0) + results_dest
       statistics_dest = args(0) + statistics_dest
       entities_dest = args(0) + entities_dest
    
      val sc = spark.sparkContext
     
      
      val w_rdd = sc.textFile(w_path)
      val f_rdd = sc.textFile(f_path)
       
      val t1 = System.currentTimeMillis()
               
      var w = loadCoordinateMatrix(w_rdd)
      var f = loadCoordinateMatrix(f_rdd)
      w_rdd.unpersist(true)
      f_rdd.unpersist(true)
      
//      val entities = sc.textFile(entities_dest).map(f =>(f.split(",")(0),f.split(",")(1)))
      
               
      val p_n = MatrixUtils.coordinateMatrixMultiply(f, w)

      val s_n_v = f.numRows()
      
      val s_i = f.numCols().toDouble / (w.numCols().toDouble * (f.numCols().toDouble + w.numCols().toDouble))
      
      val t = sc.parallelize(0 to f.numRows().toInt-1)
      
      var s_n_final = new CoordinateMatrix(t.map{ x=> 
        new MatrixEntry(x,0,s_i)})
    
         
         
      val matrix_i = new CoordinateMatrix(t.map{ x=> 
        new MatrixEntry(x,0,1)})
      
      val matrixLoadTime = (System.currentTimeMillis() - t1) / 1000
      
      var s_t_final = s_n_final
      
      var s_n_previous = s_n_final
      
      
      val epsilon = new BigDecimal(0.001)
      var distance = new BigDecimal(1)
      
      val t2 = System.currentTimeMillis()
      var iter = 0
      
      val a = MatrixUtils.multiplyMatrixByNumber(p_n, df).transpose()
      val b = MatrixUtils.divideMatrixByNumber(MatrixUtils.multiplyMatrixByNumber(matrix_i, 1-df),s_n_v.toDouble)
      
      val iter_list = new ListBuffer[Long]
      
      while( distance.compareTo(epsilon) == 1  && iter < 1000){
        val time_iter_begin = System.currentTimeMillis()
        
          s_n_previous = s_n_final
          
          s_n_final = MatrixUtils.coordinateMatrixSum(
          MatrixUtils.coordinateMatrixMultiply(a,s_n_previous),
          b)

          distance = new BigDecimal(DistanceUtils.euclideanDistance(s_n_final.entries.map(f => f.value), s_n_previous.entries.map(f => f.value)))
          
          iter = iter+1
          
          iter_list+=((System.currentTimeMillis() - time_iter_begin) / 1000)
        
      }
    

       System.gc()
      s_t_final = MatrixUtils.coordinateMatrixMultiply(f.transpose(), s_n_final)
      
   
      
//      s_n_final.toRowMatrix().rows.repartition(1).saveAsTextFile(results_dest)
  
      
      val hareTime = (System.currentTimeMillis() - t2) / 1000
      
      val statistics = new ListBuffer[String]()
      statistics += "Iterations: " + iter
      statistics += "Iteration avg time: " + computeIterTimeMean(iter_list)
      statistics += "Hare Computation Time: " + hareTime
      statistics += "Matrices Load Time: " + matrixLoadTime
      
      val rdd_statistics = sc.parallelize(statistics)
      rdd_statistics.repartition(1).saveAsTextFile(statistics_dest)
      
      
//       sc.parallelize(s_n_final.toRowMatrix().rows.flatMap(f => f.toArray).zipWithIndex().map(f => (f._2 + "e",f._1))
//      .union(s_t_final.toRowMatrix().rows.flatMap(f => f.toArray).zipWithIndex().map(f => (f._2 + "t",f._1)))
//      .join(entities).map(f => (f._2._2,f._2._1)).sortBy((f => f._2),false).top(10000)).repartition(1).saveAsTextFile(results_dest)
      
      println("Distance: " + distance)
      println("Iterations: " + iter)
      spark.stop()
  }
  
  def computeIterTimeMean(list: ListBuffer[Long]): Double = {
    var sum = 0L
    list.foreach(sum+=_)
    sum.toDouble / list.size.toDouble
  }
  
  def loadCoordinateMatrix(rdd : RDD[String]): CoordinateMatrix = {
    new CoordinateMatrix(rdd.map{ x =>
      val a = x.split(",")
      new MatrixEntry(a(0).toLong,a(1).toLong,a(2).toDouble)})
  }

  
  
}