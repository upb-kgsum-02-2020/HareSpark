package org.aksw.computations.hare

import org.aksw.utils.MatrixUtils
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import java.math.BigDecimal
import org.aksw.utils.DistanceUtils


object Hare {


  val df = 0.85

  var w_path = "/matrices/w"
  var f_path = "/matrices/f"
  var s_n_destWithProbs = "/results_hare/s_n-with-probs"
  var s_t_destWithProbs = "/results_hare/s_t-with-probs"
  var s_n_dest = "/results_hare/s_n"
  var s_t_dest = "/results_hare/s_t"
  var statistics_dest = "/hare_statistics"
  var triples_src = "/entites/triples"
  var entities_src = "/entites/entities"

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      //      .master("local[*]")
      .appName("HareScalaSpark-" + args(0).substring(args(0).lastIndexOf("/") + 1))
      .getOrCreate()

    w_path = args(0) + w_path
    f_path = args(0) + f_path
    s_n_destWithProbs = args(0) + s_n_destWithProbs
    s_t_destWithProbs = args(0) + s_t_destWithProbs
    s_n_dest = args(0) + s_n_dest
    s_t_dest = args(0) + s_t_dest
    statistics_dest = args(0) + statistics_dest
    triples_src = args(0) + triples_src
    entities_src = args(0) + entities_src

    val sc = spark.sparkContext

    // W and F transition matrices for finding P(N) -> S(N) -> S(T) -> S
    // W, triples -> entities
    val w_rdd = sc.textFile(w_path)
    // F, entities -> triples
    val f_rdd = sc.textFile(f_path)

    val t1 = System.currentTimeMillis()

    val w = loadCoordinateMatrix(w_rdd)
    val f = loadCoordinateMatrix(f_rdd)

    val strToTuple = (f: String) => {
      val fs = f.split(",", 2)
      (fs(0).toLong, fs(1))
    }
    // extract triples and entities along with their IDs
    val triples_rdd = sc.textFile(triples_src).map(strToTuple)
    val entities_rdd = sc.textFile(entities_src).map(strToTuple)

    // transition matrix P(N)
    val p_n = MatrixUtils.coordinateMatrixMultiply(f, w)

    val s_n_v = f.numRows()

    // S_0 initial entry value
    val s_i = f.numCols().toDouble / (w.numCols().toDouble * (f.numCols().toDouble + w.numCols().toDouble))

    // sequence of entities
    val t = sc.parallelize(0 until f.numRows().toInt)

    // initialize S_0 (column vector)
    var s_n_final = new CoordinateMatrix(t.map { x =>
      MatrixEntry(x, 0, s_i)
    })


    // I
    val matrix_i = new CoordinateMatrix(t.map { x =>
      MatrixEntry(x, 0, 1)
    })

    val matrixLoadTime = (System.currentTimeMillis() - t1) / 1000

    var s_t_final = s_n_final

    var s_n_previous = s_n_final


    val epsilon = new BigDecimal(0.001)
    var distance = new BigDecimal(1)

    val t2 = System.currentTimeMillis()
    var iter = 0

    val a = MatrixUtils.multiplyMatrixByNumber(p_n, df).transpose()
    val b = MatrixUtils.divideMatrixByNumber(MatrixUtils.multiplyMatrixByNumber(matrix_i, 1 - df), s_n_v.toDouble)

    val iter_list = new ListBuffer[Long]

    while (distance.compareTo(epsilon) == 1 && iter < 1000) {
      val time_iter_begin = System.currentTimeMillis()

      s_n_previous = s_n_final

      s_n_final = MatrixUtils.coordinateMatrixSum(
        MatrixUtils.coordinateMatrixMultiply(a, s_n_previous),
        b)

      distance = new BigDecimal(DistanceUtils
        .euclideanDistance(s_n_final.entries.map(f => f.value), s_n_previous.entries.map(f => f.value)))

      iter = iter + 1

      iter_list += ((System.currentTimeMillis() - time_iter_begin) / 1000)

    }


    System.gc()
    s_t_final = MatrixUtils.coordinateMatrixMultiply(f.transpose(), s_n_final)

    val joinAndMap = (a: RDD[MatrixEntry], b: RDD[(Long, String)]) => {
      a.map(x => (x.i, x.value)).join(b).map(f => (f._2._2, f._2._1))
    }

    //    sc.parallelize(topScores(joinAndMap(s_n_final.entries, entities_rdd))).repartition(1).saveAsTextFile(s_n_dest)
    //    sc.parallelize(topScores(joinAndMap(s_t_final.entries, triples_rdd))).repartition(1).saveAsTextFile(s_t_dest)
    val (s_n_mean, s_n_orig) = aboveMean(joinAndMap(s_n_final.entries, entities_rdd))
    val (s_t_mean, s_t_orig) = aboveMean(joinAndMap(s_t_final.entries, triples_rdd))
    s_n_orig.repartition(1).saveAsTextFile(s_n_destWithProbs)
    s_t_orig.repartition(1).saveAsTextFile(s_t_destWithProbs)
//    s_n_orig.map(f => f._1).repartition(1).saveAsTextFile(s_n_dest)

    val toTriple = (f: String) => {
      val Array(a, b, c) = f.split(" ", 3)
      s"<$a> <${b.substring(1)}> ${if (c.startsWith("\"")) c else s"<$c>"} ."
    }
    s_t_orig.map(f => f._1).map(toTriple).repartition(1).saveAsTextFile(s_t_dest)


    val hareTime = (System.currentTimeMillis() - t2) / 1000

    val statistics = new ListBuffer[String]()
    statistics += "Iterations: " + iter
    statistics += "Iteration avg time: " + computeIterTimeMean(iter_list)
    statistics += "Hare Computation Time: " + hareTime
    statistics += "Matrices Load Time: " + matrixLoadTime
    statistics += "Entities mean: " + s_n_mean
    statistics += "Triples mean: " + s_t_mean

    val rdd_statistics = sc.parallelize(statistics)
    rdd_statistics.repartition(1).saveAsTextFile(statistics_dest)


    spark.stop()
  }

  def computeIterTimeMean(list: ListBuffer[Long]): Double = {
    var sum = 0L
    list.foreach(sum += _)
    sum.toDouble / list.size.toDouble
  }

  def loadCoordinateMatrix(rdd: RDD[String]): CoordinateMatrix = {
    new CoordinateMatrix(rdd.map { x =>
      val Array(a, b, c) = x.split(",", 3)
      // MatrixEntry(i, j, A_ij)
      MatrixEntry(a.toLong, b.toLong, c.toDouble)
    })
  }

  def aboveMean(rdd: RDD[(String, Double)]): (Double, RDD[(String, Double)]) = {
    val mean = rdd.map(f => f._2).reduce { case (a, b) => a + b } / rdd.count()
    (mean, rdd.filter(f => f._2 > mean))
  }

  def topScores(rdd: RDD[(String, Double)]): Array[(String, Double)] = {
    rdd.sortBy(f => f._2, ascending = false).top(10000)
  }
}