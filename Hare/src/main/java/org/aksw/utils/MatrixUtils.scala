package org.aksw.utils

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object MatrixUtils {
  
  
  /**
   * 
   * Naive implementatiom to multiply sparse Matrix in Spark
   * 
   */
      def coordinateMatrixMultiply(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix): CoordinateMatrix = {
          val M_ = leftMatrix.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
          val N_ = rightMatrix.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) })
        
          val productEntries = M_
            .join(N_)
            .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
            .reduceByKey(_ + _)
            .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })
        
          new CoordinateMatrix(productEntries)
    }
      
      
        def transpose(m: Array[Array[Double]]): Array[Array[Double]] = {
          (for {
            c <- m(0).indices
          } yield m.map(_(c)) ).toArray
        }
        
        
        /**
         * 
         * Multiply all numbers
         * 
         */
        def multiplyMatrixByNumber(m: RowMatrix, n : Double) = {
            m.rows.map{x => 
              val s = x.toArray
              for(a <- 0 to s.size-1)
                {s(a) = s(a) * n}
              Vectors.dense(s)
          }
          
        }
      
      
       def transposeRowMatrix(m: RowMatrix): RowMatrix = {
            val transposedRowsRDD = m.rows.zipWithIndex.map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
              .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
              .groupByKey
              .sortByKey().map(_._2) // sort rows and remove row indexes
              .map(buildRow) // restore order of elements in each row and remove column indexes
            new RowMatrix(transposedRowsRDD)
          }
        
        
          def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
            val indexedRow = row.toArray.zipWithIndex
            indexedRow.map{case (value, colIndex) => (colIndex.toLong, (rowIndex, value))}
          }
        
          def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
            val resArr = new Array[Double](rowWithIndexes.size)
            rowWithIndexes.foreach{case (index, value) =>
                resArr(index.toInt) = value
            }
            Vectors.dense(resArr)
          } 
  
}