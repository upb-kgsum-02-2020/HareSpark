package org.aksw.computations.pagerank

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


object PageRank {
  
  
  val df = 0.85
  
  var w_path = "/matrices/w.txt"
  var f_path = "/matrices/f.txt"
  var map_edges_resources_dest = "/matrices/edges_resources.txt"
  var s_n_dest = "/results_pr/s_n.txt"
  var s_t_dest = "/results_pr/s_t.txt"
  var statistics_dest = "/pagerank_statistics"
  
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("HareImpl")
      .master("local[*]")
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
               
      
      val p_n = new CoordinateMatrix(w.entries.union(f.entries))
      

      
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
      
      
      val epsilon = new BigDecimal(0.001)
      var distance = new BigDecimal(1)
      
      val t2 = System.currentTimeMillis()
      var iter = 0L
      val ed = new EuclideanDistance
      
      val a = MatrixUtils.multiplyMatrixByNumber(p_n, df).transpose()
      var b = MatrixUtils.divideMatrixByNumber(MatrixUtils.multiplyMatrixByNumber(matrix_i, 1-df),s_n_v.toDouble)
      println("Linhas : " + b.numRows())
      
      MatrixUtils.coordinateMatrixMultiply(a,s_n_previous)
      for( it <- 0 to 10 ){
        
          s_n_previous = s_n_final
          
          var c =  MatrixUtils.coordinateMatrixMultiply(a,s_n_previous)
          
          if(c.numRows() < b.numRows()){
            val cme = sc.parallelize(Seq(new MatrixEntry(b.numRows()-1,0,0)))
            c = new CoordinateMatrix(c.entries.union(cme))
          }else if(c.numRows() > b.numRows()){
            val cme = sc.parallelize(Seq(new MatrixEntry(c.numRows()-1,0,0)))
            b = new CoordinateMatrix(b.entries.union(cme))
          }
          
          s_n_final = c.toBlockMatrix().add(b.toBlockMatrix()).toCoordinateMatrix()
          
          if(s_n_previous.numRows() > s_n_final.numRows()){
            val cme = sc.parallelize(Seq(new MatrixEntry(s_n_previous.numRows()-1,0,0)))
            s_n_final = new CoordinateMatrix(s_n_final.entries.union(cme))
          }else if(s_n_previous.numRows() < s_n_final.numRows()){
            val cme = sc.parallelize(Seq(new MatrixEntry(s_n_final.numRows()-1,0,0)))
            s_n_previous = new CoordinateMatrix(s_n_previous.entries.union(cme))
          }

//          s_n_final = 
//          MatrixUtils.coordinateMatrixMultiply(a,s_n_previous).toBlockMatrix().add(b.toBlockMatrix()).toCoordinateMatrix()
         
          val v1 = s_n_final.transpose().toRowMatrix().rows.collect()(0)
          val v2 = s_n_previous.transpose().toRowMatrix().rows.collect()(0)
      
          distance = new BigDecimal(ed.compute(v1.toArray, v2.toArray))

        iter = iter+1
      }
    
     
      s_t_final = MatrixUtils.coordinateMatrixMultiply(f.transpose(), s_n_final)
      
      s_n_final.toRowMatrix().rows.saveAsTextFile(s_n_dest)
      s_t_final.toRowMatrix().rows.saveAsTextFile(s_t_dest)
      
      val hareTime = (System.currentTimeMillis() - t2) / 1000
      
      val statistics = new ListBuffer[String]()
      statistics += "Iterations: " + iter
      statistics += "Matrices Load Time: " + matrixLoadTime
      statistics += "Hare Computation Time: " + hareTime
      
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