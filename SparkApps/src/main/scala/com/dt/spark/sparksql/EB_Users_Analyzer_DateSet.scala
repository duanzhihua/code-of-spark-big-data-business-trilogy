package com.dt.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql._


/**
  * Spark商业案例书稿第三章：3.4.3.1 电商交互式分析系统应用案例代码
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月21日；
  * 电商用户行为分析系统：在实际的生产环境下一般都是J2EE+Hadoop+Spark+DB(Redis)的方式实现的综合技术栈，在使用Spark进行电商用户行为分析的
  * 时候一般都都会是交互式的，什么是交互式的？也就是说公司内部人员例如营销部门人员向按照特定时间查询访问次数最多的用户或者购买金额最大的
  * 用户TopN,这些分析结果对于公司的决策、产品研发和营销都是至关重要的，而且很多时候是立即向要结果的，如果此时使用Hive去实现的话，可能会
  * 非常缓慢（例如1个小时），而在电商类企业中经过深度调优后的Spark一般都会比Hive快5倍以上，此时的运行时间可能就是分钟级别，这个时候就可以
  * 达到即查即用的目的，也就是所谓的交互式，而交互式的大数据系统是未来的主流！
  *
  * 我们在这里是分析电商用户的多维度的行为特征，例如分析特定时间段访问人数的TopN、特定时间段购买金额排名的TopN、注册后一周内购买金额排名TopN、
  * 注册后一周内访问次数排名Top等，但是这里的技术和业务场景同样适合于门户网站例如网易、新浪等，也同样适合于在线教育系统，例如分析在线教育系统的学员
  * 的行为，当然也适用于SNS社交网络系统，例如对于婚恋网，我们可以通过这几节课讲的内容来分析最匹配的Couple，再例如我们可以分析每周婚恋网站访问
  * 次数TopN,这个时候就可以分析出迫切想找到对象的人，婚恋网站可以基于这些分析结果进行更精准和更有效（更挣钱）的服务！
  *
  *
  * 具体数据结构如下所示：
  * User
  * |-- name: string (nullable = true)
  * |-- registeredTime: string (nullable = true)
  * |-- userID: long (nullable = true)
  * *
  * Log
  * |-- consumed: double (nullable = true)
  * |-- logID: long (nullable = true)
  * |-- time: string (nullable = true)
  * |-- typed: long (nullable = true)
  * |-- userID: long (nullable = true)
  *
  * 注意：
  * 1，在实际生产环境下要么就是Spark SQL+Parquet的方式，要么就是Spark SQL+Hive
  * 2,functions.scala这个文件包含了大量的内置函数，尤其在agg中会广泛使用，请大家无比认真反复阅读该源码并进行实践！！！
  */
object EB_Users_Analyzer_DateSet {

  case class UserLog(logID: Long, userID: Long, time: String, typed: Long, consumed: Double)

  case class LogOnce(logID: Long, userID: Long, count: Long)

  case class ConsumedOnce(logID: Long, userID: Long, consumed: Double)

  def main(args: Array[String]) {


    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[8]" //默认程序运行在本地Local模式中，主要学习和测试；

    /**
      * 当我们把程序打包运行在集群上的时候一般都会传入集群的URL信息，在这里我们假设如果传入
      * 参数的话，第一个参数只传入Spark集群的URL第二个参数传入的是数据的地址信息；
      */
    if (args.length > 0) {
      masterUrl = args(0)
    }


    /**
      * 创建Spark会话上下文SparkSession和集群上下文SparkContext，在SparkConf中可以进行各种依赖和参数的设置等，
      * 大家可以通过SparkSubmit脚本的help去看设置信息，其中SparkSession统一了Spark SQL运行的不同环境。
      */
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("EB_Users_Analyzer_DateSet")

    /**
      * SparkSession统一了Spark SQL执行时候的不同的上下文环境，也就是说Spark SQL无论运行在那种环境下我们都可以只使用
      * SparkSession这样一个统一的编程入口来处理DataFrame和DataSet编程，不需要关注底层是否有Hive等。
      */
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext //从SparkSession获得的上下文，这是因为我们读原生文件的时候或者实现一些Spark SQL目前还不支持的功能的时候需要使用SparkContext

    import org.apache.spark.sql.functions._
    //今天（2017年1月21）的第一个作业：通读functions.scala的源码
    import spark.implicits._

    //今天（2017年1月21）的第二个作业：自己根据我们的电商业务分析需要手动造出数据，需要注意的是任何实际生产环境的系统都不止一个数据文件或者不止一张表
    //例如我们这里的电商用户行为分析系统肯定至少有用户的信息usersInfo，同时肯定至少有用户访问行为信息usersAccessLog

    /**
      * 功能一：特定时间段内用户访问电商网站排名TopN:
      * 第一问题：特定时间段中的时间是从哪里来的？一般都是来自于J2EE调度系统，例如一个营销人员通过系统传入了2017.01.01~2017.01.10；
      * 第二问题：计算的时候我们会使用哪些核心算子：join、groupBy、agg（在agg中可以使用大量的functions.scala中的函数极大方便快速的实现
      * 业务逻辑系统）；
      * 第三个问题：计算完成后数据保存在哪里？现在生产环境一下是保存在DB、HBase/Canssandra、Redis等；
      *
      */

    val userInfo = spark.read.format("json").
      // json("data/sql/user.json")
      json("data/Mock_EB_Users_Data/Mock_EB_Users_Data.json")
    val userLog = spark.read.format("json").
      //json("data/sql/log.json")
      json("data/Mock_EB_Users_Data/Mock_EB_Log_Data.json")

    println("用户信息及用户访问记录文件JSON格式 :")
    userInfo.printSchema()
    userLog.printSchema()
    userLog.show()

    /**
      * 读取用户行文数据，建议使用Parquet的方式；
      **/
    /*
        val userInfo = spark.read.format("parquet").
          parquet("data/sql/userparquet.parquet")
        val userLog: DataFrame = spark.read.format("parquet").
          parquet("data/sql/logparquet.parquet")
       println("用户信息及用户访问记录文件parquet 格式 :")
       // userInfo.printSchema()
        userLog.show()
        userLog.printSchema()
      */
    /**
      * 统计特定时间段访问次数最多的Top5: 例如2016-10-01 ~ 2016-11-01
      */
    val startTime = "2016-10-01"
    val endTime = "2016-11-01"


    println("功能一 ：统计特定时间段访问次数最多的Top5: 例如2016-10-01 ~ 2016-11-01 :")
    userLog.filter("time >= '" + startTime + "' and time <= '" + endTime + "' and typed = 0")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userLog("logID")).alias("userLogCount"))
      .sort($"userLogCount".desc)
      .limit(5)
      .show()

    println("功能二：统计特定时间段内用户购买总金额排名TopN:")
    userLog.filter("time >= '" + startTime + "' and time <= '" + endTime + "'")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLog("consumed")), 2).alias("totalCount"))
      .sort($"totalCount".desc)
      .limit(10)
      .show()
    val aa: Dataset[UserLog] = userLog.as[UserLog]
    println("功能三：统计特定时间段内用户访问次数增长排名TopN:")

    val userAccessTemp: Dataset[LogOnce] = userLog.as[UserLog].filter("time >= '2016-10-08' and time <= '2016-10-14' and typed = '0'")
      .map(log => LogOnce(log.logID, log.userID, 1))
      .union(userLog.as[UserLog].filter("time >= '2016-10-01' and time <= '2016-10-07' and typed = '0'")
        .map(log => LogOnce(log.logID, log.userID, -1)))

    userAccessTemp.join(userInfo, userInfo("userID") === userAccessTemp("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userAccessTemp("count")), 2).alias("viewIncreasedTmp"))
      .sort($"viewIncreasedTmp".desc)
      .limit(10)
      .show()


    /**
      * 作业：生成Parquet方式的数据其自己实现时间函数然后测试整个代码
      * val peopleDF = spark.read.json("examples/src/main/resources/people.json")
      **/


    /**
      * 统计特定时间段购买次数最多的Top5: 例如2016-10-01 ~ 2016-11-01
      **/
    println("统计特定时间段购买次数最多的Top5: 例如2016-10-01 ~ 2016-11-01 :")
    userLog.filter("time >= '" + startTime + "' and time <= '" + endTime + "' and typed = 1")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLog("consumed")), 2).alias("totalConsumed"))
      .sort($"totalConsumed".desc)
      .limit(5)
      .show

    /**
      * 统计特定时间段里访问次数增长最多了Top5用户，例如说这一周比上一周访问次数增长最快的5位用户；
      * 实现思路：一种非常直接的方式是计算这一周每一个用户的访问次数，同时计算出上一周每个用户的访问次数，然后相减并进行排名，但是
      * 这种实现思路比较消耗性能，我们可以进行一个技能实现业务目标又能够提升性能的方式，即：把这一周的每次用户访问计数为1，把上一周
      * 的每次用户访问计数为-1，冉舟在agg操作中采用sum即可巧妙的实现增长趋势的量化
      *
      */

    val userLogDS = userLog.as[UserLog].filter("time >= '2016-10-08' and time <= '2016-10-14' and typed = '0'")
      .map(log => LogOnce(log.logID, log.userID, 1))
      .union(userLog.as[UserLog].filter("time >= '2016-10-01' and time <= '2016-10-07' and typed = '0'")
        .map(log => LogOnce(log.logID, log.userID, -1)))

    println("统计特定时间段里访问次数增长最多的Top5用户，例如说这一周比上一周访问次数增长最快的5位用户: ")
    userLogDS.join(userInfo, userLogDS("userID") === userInfo("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(sum(userLogDS("count")).alias("viewCountIncreased"))
      .sort($"viewCountIncreased".desc)
      .limit(5)
      .show()


    /**
      * 统计特定时间段里购买金额增长最多的Top5用户，例如说这一周比上一周访问次数增长最快的5位用户；
      *
      */
    println("统计特定时间段里购买金额增长最多的Top5用户，例如说这一周比上一周访问次数增长最快的5位用户: ")
    val userLogConsumerDS: Dataset[ConsumedOnce] = userLog.as[UserLog].filter("time >= '2016-10-08' and time <= '2016-10-14' and typed == 1")
      .map(log => ConsumedOnce(log.logID, log.userID, log.consumed))
      .union(userLog.as[UserLog].filter("time >= '2016-10-01' and time <= '2016-10-07' and typed == 1")
        .map(log => ConsumedOnce(log.logID, log.userID, -log.consumed)))

    userLogConsumerDS.join(userInfo, userLogConsumerDS("userID") === userInfo("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLogConsumerDS("consumed")), 2).alias("viewConsumedIncreased"))
      .sort($"viewConsumedIncreased".desc)
      .limit(5)
      .show()

    /**
      * 统计注册之后前两周内访问最多的前10个人
      */
    println("统计注册之后前两周内访问最多的前Top10:")
    userLog.join(userInfo, userInfo("userID") === userLog("userID"))
      .filter(userInfo("registeredTime") >= "2016-10-01"
        && userInfo("registeredTime") <= "2016-10-14"
        && userLog("time") >= userInfo("registeredTime")
        && userLog("time") <= date_add(userInfo("registeredTime"), 14)
        && userLog("typed") === 0)
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userLog("logID")).alias("logTimes"))
      .sort($"logTimes".desc)
      .limit(10)
      .show()


    /**
      * 统计注册之后前两周内购买总额最多的前10个人
      */
    println("统计注册之后前两周内购买总额最多的Top 10 :")
    userLog.join(userInfo, userInfo("userID") === userLog("userID"))
      .filter(userInfo("registeredTime") >= "2016-10-01"
        && userInfo("registeredTime") <= "2016-10-14"
        && userLog("time") >= userInfo("registeredTime")
        && userLog("time") <= date_add(userInfo("registeredTime"), 14)
        && userLog("typed") === 1)
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLog("consumed")), 2).alias("totalConsumed"))
      .sort($"totalConsumed".desc)
      .limit(10)
      .show()

    //    while(true){} //和通过Spark shell运行代码可以一直看到Web终端的原理是一样的，因为Spark Shell内部有一个LOOP循环

    sc.stop()


  }
}
