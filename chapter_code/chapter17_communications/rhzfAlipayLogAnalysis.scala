package com.telecom.ronghezhifu.cores

import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import rhzfLogRegex._
import scala.collection.immutable.HashSet
import scala.util.matching.Regex


/**
  * Spark商业案例书稿第17章  Spark在通信运营商生产环境中的应用案例代码
  * 版权：DT大数据梦工厂所有
  * 时间：2017年4月19日；
  *
  ***/

object rhzfAlipayLogAnalysis {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[4]"
    var dataPath = "data/ronghezhifualipaylog/logs/zhifu.log"
    var resultDataPath = "data/ronghezhifualipaylog/logsResult"

      if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("rhzfAlipayLogAnalysis")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext

    val linesConvertToGbkRDD: RDD[String] = sc.hadoopFile(dataPath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 4)
      .map(line => {
        new String(line._2.getBytes, 0, line._2.getLength, "GBK")

      })

    val linesRegexd: RDD[String] = linesConvertToGbkRDD.map(rhzfline => {
      rhzfline match {
        case RHZF_UAM_TIME_REGEX(uamLog1, uamLog2, uamLog3, uamLog4, uamLog5, uamLog6, uamLog7, uamLog8, uamLog9)
        => rhzfline
        case RHZF_MQ_QueryInvoice_ErrCode_REGEX(mqLog1, mqLog2, mqLog3, mqLog4, mqLog5, mqLog6, mqLog7, mqLog8, mqLog9, mqLog10, mqLog11, mqLog12, mqLog13, mqLog14)
        => rhzfline
        case RHZF_MQ_CHECK_ORDER_TIME_REGEX(mqTimeLog1, mqTimeLog2, mqTimeLog3, mqTimeLog4, mqTimeLog5, mqTimeLog6, mqTimeLog7, mqTimeLog8, mqTimeLog9, mqTimeLog10, mqTimeLog11)
        => rhzfline
        case RHZF_MQ_queryAccountNo_ErrCode_REGEX(mqWarnLog1, mqWarnLog2, mqWarnLog3, mqWarnLog4, mqWarnLog5, mqWarnLog6, mqWarnLog7, mqWarnLog8, mqWarnLog9, mqWarnLog10, mqWarnLog11, mqWarnLog12, mqWarnLog13, mqWarnLog14 ,mqWarnLog15)
        => rhzfline
        case _ => "MatcheIsNull"
      }
    })

    val linedFilterd: RDD[String] = linesRegexd.filter(!_.contains("MatcheIsNull"))
    linedFilterd.coalesce(1).saveAsTextFile(resultDataPath)


  }

}
