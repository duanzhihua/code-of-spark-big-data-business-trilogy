package com.dt.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.StatCounter

/** Spark商业案例书稿第四章：4.5.2 知识点：StatCounter应用案例
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月26日；
  * NBA篮球运动员大数据分析决策支持系统：
  * */

object NBAStatsMytest {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[8]"
    var dataPath = "data/NBABasketball"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("NBAStatsMytest")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    org.apache.spark.deploy.SparkHadoopUtil.get.conf.set("parquet.block.size", "new value")
    val sc = spark.sparkContext


    println("stats方法调用： ")
    val doubleRDD: RDD[Double] = sc.parallelize(Seq(100.1, 200.0, 300.0))
    val statCounter: StatCounter = doubleRDD.stats()
    println(statCounter)
    println(statCounter.merge(5000))


    val NBAStats: BballStatCounter = BballStatCounter(100)
    println(NBAStats)

    val NBAnas1 = BballStatCounter(200)
    val result: BballStatCounter = NBAnas1.add(300)
    println(result)

    val NBAnas2 = BballStatCounter(Double.NaN)
    println(NBAnas1.merge(NBAnas2))


    val arr = Array(400, Double.NaN, 500)
    val NBAnas = arr.map(X => BballStatCounter(X))
    NBAnas.foreach(println)


    val NBAnas3: Array[BballStatCounter] = Array(1.0, Double.NaN).map(d => BballStatCounter(d))
    val NBAnas4 = Array(Double.NaN, 2.0).map(d => BballStatCounter(d))
    val merged = NBAnas3.zip(NBAnas4).map { case (a, b) => a.merge(b) }

    println("====================")
    println(NBAnas3.mkString("\n"))
    println("====================")
    println(NBAnas4.mkString("\n"))
    println("=====================")
    println(merged.mkString("\n"))


    // 构建NBA测试数据
    val NBAdata: RDD[(Int, Iterable[Array[Double]])] = sc.parallelize(Seq((1984, Iterable(
      Array(-0.06829064516129113, 0.08352774193548394, 0.41606065681950233, -0.7778113892745171, 0.027867830846394284, -0.26666016861312614, -0.6326720941933309, 0.6165415556370011, -0.595782979157253),
      Array(-0.03995129032258082, -0.0023312903225805592, -0.4107265458346371, -1.2289977869201123, -0.8065836950655141, -1.4532341246428297, -0.6326720941933309, 0.6165415556370011, -1.211374091693702),
      Array(-0.08202483870967833, 0.058750645161290485, 0.41606065681950233, -0.7778113892745171, -0.4629860079253165, -0.8599471466279779, -0.6326720941933309, 0.8489158805579382, -0.4726647566499632),
      Array(-0.3832293548387099, -0.19493129032258052, -0.4107265458346371, -0.8906079886859158, -1.0520106144513695, -1.2554717986378792, -0.6326720941933309, 1.0812902054788756, -1.3191025363875806),
      Array(0.07404774193548126, 0.393021935483872, 0.41606065681950233, 0.19975913895760572, -0.2666444724166322, 0.7221514614116271, -0.8151170701932682, -1.0100787188095592, 1.2971596918923274)
    ))))


    println("NBA测试数据进行统计分析测试： ")
    val stats3: RDD[(Int, Iterable[Array[BballStatCounter]])] = NBAdata.map { case (x, y) => (x, y.map(a => a.map((b: Double) => BballStatCounter(b)))) }
    stats3.take(1).foreach(x => {
      val myX2: Iterable[Array[BballStatCounter]] = x._2
      for (i <- 1 to myX2.size) {
        val myX2size: Array[Array[BballStatCounter]] = myX2.toArray
        val myNext: Array[BballStatCounter] = myX2size(i - 1)
        for (j <- 1 to myNext.size) {
          println("第" + i + "个元素第 " + j + "个数字统计 : " + myNext(j - 1).stats)
        }
      }
    })

  }

  //stat counter class -- need printStats method to print out the stats. Useful for transformations

  class BballStatCounter extends Serializable {
    val stats: StatCounter = new StatCounter()
    var missing: Long = 0

    def add(x: Double): BballStatCounter = {
      if (x.isNaN) {
        missing += 1
      } else {
        stats.merge(x)
      }
      this
    }

    def merge(other: BballStatCounter): BballStatCounter = {
      stats.merge(other.stats)
      missing += other.missing
      this
    }

    def printStats(delim: String): String = {
      stats.count + delim + stats.mean + delim + stats.stdev + delim + stats.max + delim + stats.min
    }

    override def toString: String = {
      "NBA  stats: " + stats.toString + " NBA NaN: " + missing
    }
  }

  object BballStatCounter extends Serializable {
    def apply(x: Double) = new BballStatCounter().add(x)
  }


}
