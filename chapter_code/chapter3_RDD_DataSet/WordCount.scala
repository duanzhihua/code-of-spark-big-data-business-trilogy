package com.dt.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Partition, SparkConf, SparkContext}
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * 使用Scala开发本地测试的Spark WordCount程序
  *
  * @author DT大数据梦工厂
  *         新浪微博：http://weibo.com/ilovepains/
  */
object WordCount {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    /**
      * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
      * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
      * 只有1G的内存）的初学者       *
      */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Wow,My First Spark App!") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local") //此时，程序在本地运行，不需要安装Spark集群
    /**
      * 第2步：创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
      * 同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象
      */
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    /**
      * 第3步：根据具体的数据来源（HDFS、HBase、Local FS、DB、S3等）通过SparkContext来创建RDD
      * RDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala集合、由其它的RDD操作
      * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
      */

    val lines = sc.textFile("data/wordcount/helloSpark.txt", 1) //读取本地文件并设置为一个Partion
    /**
      * 第4步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
      * 第4.1步：讲每一行的字符串拆分成单个的单词
      */

    val words = lines.flatMap { line => line.split(" ") } //对每一行的字符串进行单词拆分并把所有行的拆分结果通过flat合并成为一个大的单词集合

    /**
      * 第4步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
      * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
      */
    val pairs = words.map { word => (word, 1) }

    /**
      * 第4步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
      * 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
      */
    val wordCountsOdered = pairs.reduceByKey(_ + _, 1000).map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1)) //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
    wordCountsOdered.collect.foreach(wordNumberPair => println(wordNumberPair._1 + " : " + wordNumberPair._2))

    sc.stop()

  }
}
