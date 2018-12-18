package com.dt.spark.Exam

import scala.collection.immutable.HashSet
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import util.control.Breaks._
import scala.collection.mutable

/**
  *  Spark商业案例书稿赠送案例：IP地址归属地查询统计.现有一批IP地址，需要根据IP地址库信息 ，查询归属地信息，并统计每一个归属地IP地址的总数
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月1日；
  **/
object IPQueryForTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[8]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("IPQueryForTest")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext
    //数据存放的目录；
    var dataPath = "data/IPQueryExam/"
    /**
      * 读取数据，用什么方式读取数据呢？在这里是使用RDD!
      */
    val IPLibRDD: RDD[String] = sc.textFile(dataPath + "IPLib_test.txt")
    val IPsRDD: RDD[String] = sc.textFile(dataPath + "IPs_test.txt")

    /**
      * 在Spark中如何实现mapjoin呢，显然是要借助于Broadcast，会把数据广播到Executor级别让该Executor上的所有任务共享
      * 该唯一的数据，而不是每次运行Task的时候都要发送一份数据的拷贝，这显著的降低了网络数据的传输和JVM内存的消耗
      */
    val IPLibSet: HashSet[String] = HashSet() ++ IPLibRDD.collect()
    val IPLibSetBroadcast: Broadcast[HashSet[String]] = sc.broadcast(IPLibSet)

    println("纯粹通过RDD的方式实现IP地址统计分析:")
    val resultIPQuery: RDD[(String, Long)] = IPsRDD.map(line => {

      var ip = line.trim
      var exists = false
      var locationKey: String = "NoMatchIPQuery"
      var IPValue: Long = 0L
      val iplibSets: HashSet[String] = IPLibSetBroadcast.value
      for (iplib <- iplibSets) {
        val ipSection: String = iplib.split("\t")(0).trim + "-" + iplib.split("\t")(1).trim
        breakable {

          exists = com.dt.spark.chinatelecomExam.IpUtil.ipExistsInRange(ip, ipSection)
          if (exists == true) {
            locationKey = iplib.split("\t")(2).trim + "\t" + iplib.split("\t")(3).trim
            IPValue = 1L
            break()
          }
        }
      }
      (locationKey, IPValue)
    })

    val result = resultIPQuery.filter(!_._1.contains("NoMatchIPQuery")).reduceByKey(_ + _)
    result.map(line =>
      line._1 + "\t" + line._2).saveAsTextFile(dataPath + "IPQueryResult.txt")
  }
}
