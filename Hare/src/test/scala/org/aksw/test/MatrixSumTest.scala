package org.aksw.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.aksw.utils.MatrixUtils

object MatrixSumTest {
  
  def main(args: Array[String]): Unit = {
    
    
     val conf = new SparkConf
     conf.setAppName("MatrixSumTest")
     conf.setMaster("local[*]")
     
     val sc = new SparkContext(conf)
     
     val me1_1 = new MatrixEntry(0,0,1.5)
     val me1_2 = new MatrixEntry(0,1,1)
     val me1_3 = new MatrixEntry(0,2,1)
     
     val me1_4 = new MatrixEntry(1,0,1)
     val me1_5 = new MatrixEntry(1,1,1)
     val me1_6 = new MatrixEntry(1,2,1)
     
     val me2_1 = new MatrixEntry(0,0,1.5)
     val me2_2 = new MatrixEntry(0,1,1)
     val me2_3 = new MatrixEntry(0,2,1)
     
     val me2_4 = new MatrixEntry(1,0,1)
     val me2_5 = new MatrixEntry(1,1,1)
     val me2_6 = new MatrixEntry(1,2,.5)
     
     
     val m_1 = new CoordinateMatrix(sc.parallelize( Array(me1_1,me1_2,me1_3,me1_4,me1_5,me1_6)))
     val m_2 = new CoordinateMatrix(sc.parallelize( Array(me2_1,me2_2,me2_3,me2_4,me2_5,me2_6)))
     
     val m_3 = MatrixUtils.coordinateMatrixSum(m_1, m_2)
     
     m_3.entries.foreach(println)
    
  }
  
}