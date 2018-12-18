package com.dt.spark.sparksql

import scala.language.postfixOps
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.{Map, mutable}

/**
  * Spark商业案例书稿第四章：4.4  NBA篮球运动员大数据分析完整代码测试和实战
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月26日；
  * NBA篮球运动员大数据分析决策支持系统：
  * 基于NBA球员历史数据1970~2017年各种表现，全方位分析球员的技能，构建最强NBA篮球团队做数据分析支撑系统
  * 曾经非常火爆的梦幻篮球是基于现实中的篮球比赛数据根据对手的情况制定游戏的先发阵容和比赛结果（也就是说比赛结果是由实际结果来决定），
  * 游戏中可以管理球员，例如说调整比赛的阵容，其中也包括裁员、签入和交易等
  *
  * 而这里的大数据分析系统可以被认为是游戏背后的数据分析系统。
  * 具体的数据关键的数据项如下所示：
  * 3P：3分命中；
  * 3PA：3分出手；
  * 3P%：3分命中率；
  * 2P：2分命中；
  * 2PA：2分出手；
  * 2P%：2分命中率；
  * TRB：篮板球；
  * STL：抢断；
  * AST：助攻；
  * BLT: 盖帽；
  * FT: 罚球命中；
  * TOV: 失误；
  *
  *
  * 基于球员的历史数据，如何对球员进行评价？也就是如何进行科学的指标计算，一个比较流行的算法是Z-score：其基本的计算过程是
  * 基于球员的得分减去平均值后来除以标准差，举个简单的例子，某个球员在2016年的平均篮板数是7.1，而所有球员在2016年的平均篮板数是4.5
  * 而标准差是1.3，那么该球员Z-score得分为：2
  *
  * 在计算球员的表现指标中可以计算FT%、BLK、AST、FG%等；
  *
  *
  * 具体如何通过Spark技术来实现呢？
  * 第一步：数据预处理：例如去掉不必要的标题等信息；
  * 第二步：数据的缓存：为加速后面的数据处理打下基础；
  * 第三步：基础数据项计算：方差、均值、最大值、最小值、出现次数等等；
  * 第四步：计算Z-score，一般会进行广播，可以提升效率；
  * 第五步：基于前面四步的基础可以借助Spark SQL进行多维度NBA篮球运动员数据分析，可以使用SQL语句，也可以使用DataSet（我们在这里可能会
  * 优先选择使用SQL，为什么呢？其实原因非常简单，复杂的算法级别的计算已经在前面四步完成了且广播给了集群，我们在SQL中可以直接使用）
  * 第六步：把数据放在Redis或者DB中；
  *
  *
  * Tips：
  * 1，这里的一个非常重要的实现技巧是通过RDD计算出来一些核心基础数据并广播出去，后面的业务基于SQL去实现，既简单又可以灵活的应对业务变化需求，希望
  * 大家能够有所启发；
  * 2，使用缓存和广播以及调整并行度等来提升效率；
  *
  */
object NBABasketball_Analysis {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[4]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a SparContext with the given master URL
    /**
      * Spark SQL默认情况下Shuffle的时候并行度是200，如果数据量不是非常多的情况下，设置200的Shuffle并行度会拖慢速度，
      * 所以在这里我们根据实际情况进行了调整，因为NBA的篮球运动员的数据并不是那么多，这样做同时也可以让机器更有效的使用（例如内存等）
      */
    val conf = new SparkConf().setMaster(masterUrl).set("spark.sql.shuffle.partitions", "5").setAppName("FantasyBasketball")
    val spark = SparkSession
      .builder()
      .appName("NBABasketball_Analysis")
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    //********************
    //SET-UP
    //********************


    val DATA_PATH = "data/NBABasketball"
    //数据存在的目录
    val TMP_PATH = "data/basketball_tmp"

    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(TMP_PATH), true)

    //process files so that each line includes the year
    for (i <- 1970 to 2016) {
      println(i)
      val yearStats = sc.textFile(s"${DATA_PATH}/leagues_NBA_$i*").repartition(sc.defaultParallelism)
      yearStats.filter(x => x.contains(",")).map(x => (i, x)).saveAsTextFile(s"${TMP_PATH}/BasketballStatsWithYear/$i/")
    }


    //********************
    //CODE
    //********************
    //Cut and Paste into the Spark Shell. Use :paste to enter "cut and paste mode" and CTRL+D to process
    //spark-shell --master yarn-client
    //********************


    //********************
    //Classes, Helper Functions + Variables
    //********************
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    import org.apache.spark.util.StatCounter

    import scala.collection.mutable.ListBuffer

    //helper funciton to compute normalized value
    def statNormalize(stat: Double, max: Double, min: Double) = {
      val newmax = math.max(math.abs(max), math.abs(min))
      stat / newmax
    }

    //Holds initial bball stats + weighted stats + normalized stats
    case class BballData(val year: Int, name: String, position: String,
                         age: Int, team: String, gp: Int, gs: Int, mp: Double,
                         stats: Array[Double], statsZ: Array[Double] = Array[Double](),
                         valueZ: Double = 0, statsN: Array[Double] = Array[Double](),
                         valueN: Double = 0, experience: Double = 0)

    //parse a stat line into a BBallDataZ object
    def bbParse(input: String, bStats: scala.collection.Map[String, Double] = Map.empty,
                zStats: scala.collection.Map[String, Double] = Map.empty): BballData = {
      val line = input.replace(",,", ",0,")
      val pieces = line.substring(1, line.length - 1).split(",")
      val year = pieces(0).toInt
      val name = pieces(2)
      val position = pieces(3)
      val age = pieces(4).toInt
      val team = pieces(5)
      val gp = pieces(6).toInt
      val gs = pieces(7).toInt
      val mp = pieces(8).toDouble

      val stats: Array[Double] = pieces.slice(9, 31).map(x => x.toDouble)
      var statsZ: Array[Double] = Array.empty
      var valueZ: Double = Double.NaN
      var statsN: Array[Double] = Array.empty
      var valueN: Double = Double.NaN

      if (!bStats.isEmpty) {
        val fg: Double = (stats(2) - bStats.apply(year.toString + "_FG%_avg")) * stats(1)
        val tp = (stats(3) - bStats.apply(year.toString + "_3P_avg")) / bStats.apply(year.toString + "_3P_stdev")
        val ft = (stats(12) - bStats.apply(year.toString + "_FT%_avg")) * stats(11)
        val trb = (stats(15) - bStats.apply(year.toString + "_TRB_avg")) / bStats.apply(year.toString + "_TRB_stdev")
        val ast = (stats(16) - bStats.apply(year.toString + "_AST_avg")) / bStats.apply(year.toString + "_AST_stdev")
        val stl = (stats(17) - bStats.apply(year.toString + "_STL_avg")) / bStats.apply(year.toString + "_STL_stdev")
        val blk = (stats(18) - bStats.apply(year.toString + "_BLK_avg")) / bStats.apply(year.toString + "_BLK_stdev")
        val tov = (stats(19) - bStats.apply(year.toString + "_TOV_avg")) / bStats.apply(year.toString + "_TOV_stdev") * (-1)
        val pts = (stats(21) - bStats.apply(year.toString + "_PTS_avg")) / bStats.apply(year.toString + "_PTS_stdev")
        statsZ = Array(fg, ft, tp, trb, ast, stl, blk, tov, pts)
        valueZ = statsZ.reduce(_ + _)

        if (!zStats.isEmpty) {
          val zfg = (fg - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev")
          val zft = (ft - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev")
          val fgN = statNormalize(zfg, (zStats.apply(year.toString + "_FG_max") - zStats.apply(year.toString + "_FG_avg"))
            / zStats.apply(year.toString + "_FG_stdev"), (zStats.apply(year.toString + "_FG_min")
            - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev"))
          val ftN = statNormalize(zft, (zStats.apply(year.toString + "_FT_max") - zStats.apply(year.toString + "_FT_avg"))
            / zStats.apply(year.toString + "_FT_stdev"), (zStats.apply(year.toString + "_FT_min")
            - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev"))
          val tpN = statNormalize(tp, zStats.apply(year.toString + "_3P_max"), zStats.apply(year.toString + "_3P_min"))
          val trbN = statNormalize(trb, zStats.apply(year.toString + "_TRB_max"), zStats.apply(year.toString + "_TRB_min"))
          val astN = statNormalize(ast, zStats.apply(year.toString + "_AST_max"), zStats.apply(year.toString + "_AST_min"))
          val stlN = statNormalize(stl, zStats.apply(year.toString + "_STL_max"), zStats.apply(year.toString + "_STL_min"))
          val blkN = statNormalize(blk, zStats.apply(year.toString + "_BLK_max"), zStats.apply(year.toString + "_BLK_min"))
          val tovN = statNormalize(tov, zStats.apply(year.toString + "_TOV_max"), zStats.apply(year.toString + "_TOV_min"))
          val ptsN = statNormalize(pts, zStats.apply(year.toString + "_PTS_max"), zStats.apply(year.toString + "_PTS_min"))
          statsZ = Array(zfg, zft, tp, trb, ast, stl, blk, tov, pts)
          //  println("bbParse函数中打印statsZ： " + statsZ.foreach(println(_)) )
          valueZ = statsZ.reduce(_ + _)
          statsN = Array(fgN, ftN, tpN, trbN, astN, stlN, blkN, tovN, ptsN)
          //   println("bbParse函数中打印statsN： " + statsN.foreach(println(_)) )
          valueN = statsN.reduce(_ + _)
        }
      }
      BballData(year, name, position, age, team, gp, gs, mp, stats, statsZ, valueZ, statsN, valueN)
    }

    //stat counter class -- need printStats method to print out the stats. Useful for transformations
    //该类是一个辅助工具类，在后面编写业务代码的时候会反复使用其中的方法
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
        "stats: " + stats.toString + " NaN: " + missing
      }
    }

    object BballStatCounter extends Serializable {
      def apply(x: Double) = new BballStatCounter().add(x) //在这里使用了Scala语言的一个编程技巧，借助于apply工厂方法，在构造该对象的时候就可以执行出结果
    }

    //process raw data into zScores and nScores
    def processStats(stats0: org.apache.spark.rdd.RDD[String], txtStat: Array[String],
                     bStats: scala.collection.Map[String, Double] = Map.empty,
                     zStats: scala.collection.Map[String, Double] = Map.empty): RDD[(String, Double)] = {
      //parse stats
      val stats1: RDD[BballData] = stats0.map(x => bbParse(x, bStats, zStats))

      //group by year
      val stats2: RDD[(Int, Iterable[Array[Double]])] = {
        if (bStats.isEmpty) {
          stats1.keyBy(x => x.year).map(x => (x._1, x._2.stats)).groupByKey()
        } else {
          stats1.keyBy(x => x.year).map(x => (x._1, x._2.statsZ)).groupByKey()
        }
      }

      println("=====测试一下stats统计类==============：   ")
      stats2.take(5).foreach(x => {
        val myx2: Iterable[Array[Double]] = x._2
        println(" 遍历Iterable：" + myx2.size)
        for (i <- 1 to myx2.size) {
          val myx2size: Array[Array[Double]] = myx2.toArray
          val mynext: Array[Double] = myx2size(i - 1)
          println(i + " : " + x._1 + " , 循环遍历   " + mynext.mkString(" || "))
        }

      })


      //map each stat to StatCounter
      val stats3: RDD[(Int, Iterable[Array[BballStatCounter]])] = stats2.map { case (x, y) => (x, y.map(a => a.map((b: Double) => BballStatCounter(b)))) }

      //merge all stats together
      val stats4: RDD[(Int, Array[BballStatCounter])] = stats3.map { case (x, y) => (x, y.reduce((a, b) => a.zip(b).map { case (c, d) => c.merge(d) })) }

      //combine stats with label and pull label out
      val stats5: RDD[Array[(Int, String, BballStatCounter)]] = stats4.map { case (x, y: Array[BballStatCounter]) => (x, txtStat.zip(y)) }.map {
        x =>
          (x._2.map {
            case (y, z) => (x._1, y, z)
          })
      }

      //separate each stat onto its own line and print out the Stats to a String
      val stats6: RDD[(Int, String, String)] = stats5.flatMap(x => x.map(y => (y._1, y._2, y._3.printStats(","))))
      //
      println("测试一下转换========")
      stats6.take(1).foreach(println)
      //turn stat tuple into key-value pairs with corresponding agg stat
      val stats7: RDD[(String, Double)] = stats6.flatMap { case (a, b, c) => {
        val pieces = c.split(",")
        val count = pieces(0)
        val mean = pieces(1)
        val stdev = pieces(2)
        val max = pieces(3)
        val min = pieces(4)
        /*    println("processStats函数的返回结果array" +
              (a + "_" + b + "_" + "count", count.toDouble),
              (a + "_" + b + "_" + "avg", mean.toDouble),
              (a + "_" + b + "_" + "stdev", stdev.toDouble),
              (a + "_" + b + "_" + "max", max.toDouble),
              (a + "_" + b + "_" + "min", min.toDouble))*/


        Array((a + "_" + b + "_" + "count", count.toDouble),
          (a + "_" + b + "_" + "avg", mean.toDouble),
          (a + "_" + b + "_" + "stdev", stdev.toDouble),
          (a + "_" + b + "_" + "max", max.toDouble),
          (a + "_" + b + "_" + "min", min.toDouble))
      }
      }
      stats7
    }

    //process stats for age or experience
    def processStatsAgeOrExperience(stats0: org.apache.spark.rdd.RDD[(Int, Array[Double])], label: String): DataFrame = {


      //group elements by age
      val stats1: RDD[(Int, Iterable[Array[Double]])] = stats0.groupByKey()

      val stats2: RDD[(Int, Iterable[Array[BballStatCounter]])] = stats1.map {
        case (x: Int, y: Iterable[Array[Double]]) =>
          (x, y.map((z: Array[Double]) => z.map((a: Double) => BballStatCounter(a))))
      }
      //Reduce rows by merging StatCounter objects
      val stats3: RDD[(Int, Array[BballStatCounter])] = stats2.map { case (x, y) => (x, y.reduce((a, b) => a.zip(b).map { case (c, d) => c.merge(d) })) }
      //turn data into RDD[Row] object for dataframe
      val stats4 = stats3.map(x => Array(Array(x._1.toDouble),
        x._2.flatMap(y => y.printStats(",").split(",")).map(y => y.toDouble)).flatMap(y => y))
        .map(x =>
          Row(x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8),
            x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20)))

      //create schema for age table
      val schema = StructType(
        StructField(label, IntegerType, true) ::
          StructField("valueZ_count", DoubleType, true) ::
          StructField("valueZ_mean", DoubleType, true) ::
          StructField("valueZ_stdev", DoubleType, true) ::
          StructField("valueZ_max", DoubleType, true) ::
          StructField("valueZ_min", DoubleType, true) ::
          StructField("valueN_count", DoubleType, true) ::
          StructField("valueN_mean", DoubleType, true) ::
          StructField("valueN_stdev", DoubleType, true) ::
          StructField("valueN_max", DoubleType, true) ::
          StructField("valueN_min", DoubleType, true) ::
          StructField("deltaZ_count", DoubleType, true) ::
          StructField("deltaZ_mean", DoubleType, true) ::
          StructField("deltaZ_stdev", DoubleType, true) ::
          StructField("deltaZ_max", DoubleType, true) ::
          StructField("deltaZ_min", DoubleType, true) ::
          StructField("deltaN_count", DoubleType, true) ::
          StructField("deltaN_mean", DoubleType, true) ::
          StructField("deltaN_stdev", DoubleType, true) ::
          StructField("deltaN_max", DoubleType, true) ::
          StructField("deltaN_min", DoubleType, true) :: Nil
      )

      //create data frame
      spark.createDataFrame(stats4, schema)
    }

    //********************
    //Processing + Transformations
    //********************


    //********************
    //Compute Aggregate Stats Per Year
    //********************

    //read in all stats
    val stats = sc.textFile(s"${TMP_PATH}/BasketballStatsWithYear/*/*").repartition(sc.defaultParallelism)

    //filter out junk rows, clean up data entry errors as well
    val filteredStats: RDD[String] = stats.filter(x => !x.contains("FG%")).filter(x => x.contains(","))
      .map(x => x.replace("*", "").replace(",,", ",0,"))
    filteredStats.cache()
    println("NBA球员清洗以后的数据记录:  ")
    filteredStats.take(10).foreach(println)

    //process stats and save as map
    val txtStat: Array[String] = Array("FG", "FGA", "FG%", "3P", "3PA", "3P%", "2P", "2PA", "2P%", "eFG%", "FT",
      "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS")
    println("NBA球员数据统计维度: ")
    txtStat.foreach(println)
    val aggStats: Map[String, Double] = processStats(filteredStats, txtStat).collectAsMap //基础数据项，需要在集群中使用，因此会在后面广播出去
    println("NBA球员基础数据项aggStats MAP映射集: ")
    aggStats.take(60).foreach { case (k, v) => println(" （ " + k + "  , " + v + " ) ") }

    //collect rdd into map and broadcast
    val broadcastStats: Broadcast[Map[String, Double]] = sc.broadcast(aggStats) //使用广播提升效率


    //********************
    //Compute Z-Score Stats Per Year
    //********************

    //parse stats, now tracking weights
    val txtStatZ = Array("FG", "FT", "3P", "TRB", "AST", "STL", "BLK", "TOV", "PTS")
    val zStats: Map[String, Double] = processStats(filteredStats, txtStatZ, broadcastStats.value).collectAsMap
    println("NBA球员Z-Score标准分zStats  MAP映射集: ")
    zStats.take(10).foreach { case (k, v) => println(" （ " + k + "  , " + v + " ) ") }
    //collect rdd into map and broadcast
    val zBroadcastStats = sc.broadcast(zStats)


    //********************
    //Compute Normalized Stats Per Year
    //********************

    //parse stats, now normalizing
    val nStats: RDD[BballData] = filteredStats.map(x => bbParse(x, broadcastStats.value, zBroadcastStats.value))

    //map RDD to RDD[Row] so that we can turn it into a dataframe

    val nPlayer: RDD[Row] = nStats.map(x => {
      val nPlayerRow: Row = Row.fromSeq(Array(x.name, x.year, x.age, x.position, x.team, x.gp, x.gs, x.mp)
        ++ x.stats ++ x.statsZ ++ Array(x.valueZ) ++ x.statsN ++ Array(x.valueN))
      //println( nPlayerRow.mkString)
      nPlayerRow
    })

    //create schema for the data frame
    val schemaN: StructType = StructType(
      StructField("name", StringType, true) ::
        StructField("year", IntegerType, true) ::
        StructField("age", IntegerType, true) ::
        StructField("position", StringType, true) ::
        StructField("team", StringType, true) ::
        StructField("gp", IntegerType, true) ::
        StructField("gs", IntegerType, true) ::
        StructField("mp", DoubleType, true) ::
        StructField("FG", DoubleType, true) ::
        StructField("FGA", DoubleType, true) ::
        StructField("FGP", DoubleType, true) ::
        StructField("3P", DoubleType, true) ::
        StructField("3PA", DoubleType, true) ::
        StructField("3PP", DoubleType, true) ::
        StructField("2P", DoubleType, true) ::
        StructField("2PA", DoubleType, true) ::
        StructField("2PP", DoubleType, true) ::
        StructField("eFG", DoubleType, true) ::
        StructField("FT", DoubleType, true) ::
        StructField("FTA", DoubleType, true) ::
        StructField("FTP", DoubleType, true) ::
        StructField("ORB", DoubleType, true) ::
        StructField("DRB", DoubleType, true) ::
        StructField("TRB", DoubleType, true) ::
        StructField("AST", DoubleType, true) ::
        StructField("STL", DoubleType, true) ::
        StructField("BLK", DoubleType, true) ::
        StructField("TOV", DoubleType, true) ::
        StructField("PF", DoubleType, true) ::
        StructField("PTS", DoubleType, true) ::
        StructField("zFG", DoubleType, true) ::
        StructField("zFT", DoubleType, true) ::
        StructField("z3P", DoubleType, true) ::
        StructField("zTRB", DoubleType, true) ::
        StructField("zAST", DoubleType, true) ::
        StructField("zSTL", DoubleType, true) ::
        StructField("zBLK", DoubleType, true) ::
        StructField("zTOV", DoubleType, true) ::
        StructField("zPTS", DoubleType, true) ::
        StructField("zTOT", DoubleType, true) ::
        StructField("nFG", DoubleType, true) ::
        StructField("nFT", DoubleType, true) ::
        StructField("n3P", DoubleType, true) ::
        StructField("nTRB", DoubleType, true) ::
        StructField("nAST", DoubleType, true) ::
        StructField("nSTL", DoubleType, true) ::
        StructField("nBLK", DoubleType, true) ::
        StructField("nTOV", DoubleType, true) ::
        StructField("nPTS", DoubleType, true) ::
        StructField("nTOT", DoubleType, true) :: Nil
    )

    //create data frame
    val dfPlayersT: DataFrame = spark.createDataFrame(nPlayer, schemaN)

    //save all stats as a temp table
    dfPlayersT.createOrReplaceTempView("tPlayers")

    //calculate exp and zdiff, ndiff
    val dfPlayers: DataFrame = spark.sql("select age-min_age as exp,tPlayers.* from tPlayers join" +
      " (select name,min(age)as min_age from tPlayers group by name) as t1" +
      " on tPlayers.name=t1.name order by tPlayers.name, exp  ")
    println("计算exp and zdiff, ndiff")
    dfPlayers.show()
    //save as table
    dfPlayers.createOrReplaceTempView("Players")
    //filteredStats.unpersist()

    //********************
    //ANALYSIS
    //********************
    println("打印NBA球员的历年比赛记录：   ")
    dfPlayers.rdd.map(x =>
      (x.getString(1), x)).filter(_._1.contains("A.C. Green")).foreach(println)

    val pStats: RDD[(String, Iterable[(Double, Double, Int, Int, Array[Double], Int)])] = dfPlayers.sort(dfPlayers("name"), dfPlayers("exp") asc).rdd.map(x =>
      (x.getString(1), (x.getDouble(50), x.getDouble(40), x.getInt(2), x.getInt(3),
        Array(x.getDouble(31), x.getDouble(32), x.getDouble(33), x.getDouble(34), x.getDouble(35),
          x.getDouble(36), x.getDouble(37), x.getDouble(38), x.getDouble(39)), x.getInt(0))))
      .groupByKey
    pStats.cache

    println("**********根据NBA球员名字分组：   ")
    pStats.take(15).foreach(x => {
      val myx2: Iterable[(Double, Double, Int, Int, Array[Double], Int)] = x._2
      println("按NBA球员： " + x._1 + " 进行分组，组中元素个数为：" + myx2.size)
      for (i <- 1 to myx2.size) {
        val myx2size: Array[(Double, Double, Int, Int, Array[Double], Int)] = myx2.toArray
        val mynext: (Double, Double, Int, Int, Array[Double], Int) = myx2size(i - 1)
        println(i + " : " + x._1 + " , while   " + mynext._1 + " , " + mynext._2 + " , "
          + mynext._3 + " , " + mynext._4 + " ,     " + mynext._5.mkString(" || ") + "     , "
          + mynext._6)
      }

    })


    import spark.implicits._
    //for each player, go through all the years and calculate the change in valueZ and valueN, save into two lists
    //one for age, one for experience
    //exclude players who played in 1980 from experience, as we only have partial data for them
    val excludeNames: String = dfPlayers.filter(dfPlayers("year") === 1980).select(dfPlayers("name"))
      .map(x => x.mkString).collect().mkString(",")

    val pStats1: RDD[(ListBuffer[(Int, Array[Double])], ListBuffer[(Int, Array[Double])])] = pStats.map { case (name, stats) =>
      var last = 0
      var deltaZ = 0.0
      var deltaN = 0.0
      var valueZ = 0.0
      var valueN = 0.0
      var exp = 0
      val aList = ListBuffer[(Int, Array[Double])]()
      val eList = ListBuffer[(Int, Array[Double])]()
      stats.foreach(z => {
        if (last > 0) {
          deltaN = z._1 - valueN
          deltaZ = z._2 - valueZ
        } else {
          deltaN = Double.NaN
          deltaZ = Double.NaN
        }
        valueN = z._1
        valueZ = z._2
        last = z._4
        aList += ((last, Array(valueZ, valueN, deltaZ, deltaN)))
        if (!excludeNames.contains(z._1)) {
          exp = z._6
          eList += ((exp, Array(valueZ, valueN, deltaZ, deltaN)))
        }
      })
      (aList, eList)
    }

    pStats1.cache


    println("按NBA球员的年龄及经验值进行统计：   ")
    pStats1.take(10).foreach(x => {
      //pStats1: RDD[(ListBuffer[(Int, Array[Double])], ListBuffer[(Int, Array[Double])])]
      for (i <- 1 to x._1.size) {
        println("年龄：" + x._1(i - 1)._1 + " , " + x._1(i - 1)._2.mkString("||") +
          "  经验: " + x._2(i - 1)._1 + " , " + x._2(i - 1)._2.mkString("||"))
      }
    })


    //********************
    //compute age stats
    //********************

    //extract out the age list
    val pStats2: RDD[(Int, Array[Double])] = pStats1.flatMap { case (x, y) => x }

    //create age data frame
    val dfAge: DataFrame = processStatsAgeOrExperience(pStats2, "age")
    dfAge.show()
    //save as table
    dfAge.createOrReplaceTempView("Age")

    //extract out the experience list
    val pStats3: RDD[(Int, Array[Double])] = pStats1.flatMap { case (x, y) => y }

    //create experience dataframe
    val dfExperience: DataFrame = processStatsAgeOrExperience(pStats3, "Experience")
    dfExperience.show()
    //save as table
    dfExperience.createOrReplaceTempView("Experience")

    pStats1.unpersist()

    //while(true){}
  }

}