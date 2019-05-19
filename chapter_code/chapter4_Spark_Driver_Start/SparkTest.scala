package com.dt.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}


 
object SparkTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ALL)
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Wow,My First Spark App!") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local-cluster[1, 1, 1024]")
    //conf.setMaster("local")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    println(sc.parallelize(Array("100", "200"), 1).count())
    while (true) {}
    sc.stop()

  }
}
