package org.aksw.graph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object DataFrame {
  
  def main(args: Array[String]): Unit = {
    
     val spark = SparkSession.builder
       .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .appName("DF_GraphX")
      .getOrCreate()
      
      val df_1 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/gsjunior/test_data/2008.csv")
      
      df_1.show
    
//    val conf = new SparkConf
//    conf.setAppName("")
//    conf.setMaster("local[*]")
//    
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    
//    val df_1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/home/gsjunior/test_data/2008.csv")
//    
//    df_1.show
    
  }
  
}