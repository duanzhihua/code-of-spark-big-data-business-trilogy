package com.dt.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


/**Spark商业案例书稿第四章： 4.3.1 NBA球员数据每年基础数据项记录
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月26日；
  * NBA篮球运动员大数据分析决策支持系统：
  *   基于NBA球员历史数据1970~2017年各种表现，全方位分析球员的技能，构建最强NBA篮球团队做数据分析支撑系统
  * 曾经非常火爆的梦幻篮球是基于现实中的篮球比赛数据根据对手的情况制定游戏的先发阵容和比赛结果（也就是说比赛结果是由实际结果来决定），
  * 游戏中可以管理球员，例如说调整比赛的阵容，其中也包括裁员、签入和交易等
  *
  * 而这里的大数据分析系统可以被认为是游戏背后的数据分析系统。
  * 具体的数据关键的数据项如下所示：
  *   3P：3分命中；
  *   3PA：3分出手；
  *   3P%：3分命中率；
  *   2P：2分命中；
  *   2PA：2分出手；
  *   2P%：2分命中率；
  *   TRB：篮板球；
  *   STL：抢断；
  *   AST：助攻；
  *   BLT: 盖帽；
  *   FT: 罚球命中；
  *   TOV: 失误；
  *
  *
  *   基于球员的历史数据，如何对球员进行评价？也就是如何进行科学的指标计算，一个比较流行的算法是Z-score：其基本的计算过程是
  *     基于球员的得分减去平均值后来除以标准差，举个简单的例子，某个球员在2016年的平均篮板数是7.1，而所有球员在2016年的平均篮板数是4.5
  *     而标准差是1.3，那么该球员Z-score得分为：2
  *
  *   在计算球员的表现指标中可以计算FT%、BLK、AST、FG%等；
  *
  *
  *   具体如何通过Spark技术来实现呢？
  *   第一步：数据预处理：例如去掉不必要的标题等信息；
  *   第二步：数据的缓存：为加速后面的数据处理打下基础；
  *   第三步：基础数据项计算：方差、均值、最大值、最小值、出现次数等等；
  *   第四步：计算Z-score，一般会进行广播，可以提升效率；
  *   第五步：基于前面四步的基础可以借助Spark SQL进行多维度NBA篮球运动员数据分析，可以使用SQL语句，也可以使用DataSet（我们在这里可能会
  *     优先选择使用SQL，为什么呢？其实原因非常简单，复杂的算法级别的计算已经在前面四步完成了且广播给了集群，我们在SQL中可以直接使用）
  *   第六步：把数据放在Redis或者DB中；
  *
  *
  *   Tips：
  *     1，这里的一个非常重要的实现技巧是通过RDD计算出来一些核心基础数据并广播出去，后面的业务基于SQL去实现，既简单又可以灵活的应对业务变化需求，希望
  *       大家能够有所启发；
  *     2，使用缓存和广播以及调整并行度等来提升效率；
  *
  */
object NBAPlayer_Analyzer_DateSet {


  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR) //日志过滤级别，我们在控制台上只打印正确的结果或错误的信息

    var masterUrl = "local[8]" //默认程序运行在本地Local模式中，主要学习和测试；

    /**
      * 当我们把程序打包运行在集群上的时候一般都会传入集群的URL信息，在这里我们假设如果传入
      * 参数的话，第一个参数只传入Spark集群的URL第二个参数传入的是数据的地址信息；
      */
    if(args.length > 0) {
      masterUrl = args(0) //如果代码提交到集群上运行，传入给SparkSubmit的第一个参数需要是集群master的地址
    }


    /**
      * 创建Spark会话上下文SparkSession和集群上下文SparkContext，在SparkConf中可以进行各种依赖和参数的设置等，
      * 大家可以通过SparkSubmit脚本的help去看设置信息，其中SparkSession统一了Spark SQL运行的不同环境。
      */
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("NBAPlayer_Analyzer_DateSet")

    /**
      * SparkSession统一了Spark SQL执行时候的不同的上下文环境，也就是说Spark SQL无论运行在那种环境下我们都可以只使用
      * SparkSession这样一个统一的编程入口来处理DataFrame和DataSet编程，不需要关注底层是否有Hive等。
      * 有了SparkSession之后就不在需要SqlContext或者HiveContext
      */
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext //从SparkSession获得的上下文，这是因为我们读原生文件的时候或者实现一些Spark SQL目前还不支持的功能的时候需要使用SparkContext


    /**
      * 第一步：对原始的NBA球员数据进行初步的处理并进行缓存
      *
      */
    val data_Path = "data/NBABasketball"  //数据存在的目录
    val data_Tmp = "data/basketball_tmp"
    /**
      * 因为文件非常多，此时我们需要采用循环来读取所有文件的数据
      */
    FileSystem.get(new Configuration()).delete(new Path(data_Tmp),true) //如果临时文件夹已经存在，在删除其中的数据
    for(year <- 1970 to 1970){
      //for(year <- 1970 to 2016){
      val statsPerYear = sc.textFile(s"${data_Path}/leagues_NBA_${year}*")  //通过路径组拼读取每一年NBA球员的数据信息
      statsPerYear.filter(_.contains(",")).map(line => (year, line)).
        saveAsTextFile(s"${data_Tmp}/NBAStatsPerYear/${year}/")
    }
    val NBAStats = sc.textFile(s"${data_Tmp}/NBAStatsPerYear/*/*")  //读取进来所有的所有的NBA球员过去的历史数据
    /**
      * 进行数据初步的ETL清洗工作，实际产生的数据可能是不符合处理要求的数据，需要我们按照一定的规则进行清洗和格式化
      * 完成这个工作的关键是清晰的知道数据是如何产生的以及我们需要什么样的数据。
      */
    val filteredData: RDD[String] = NBAStats.filter(line => !line.contains("FG%")).filter(line => line.contains(",")).
      map(line => line.replace(",,", ",0,")) //数据清理的工作可能是持续的

    /**
      * 数据缓存，为了加快后续的处理进度，我们一般把反复使用的数据都需要进行缓存处理
      * 老师比较推荐使用StorageLevel.MEMORY_AND_DISK，因为这样可以更好的使用内存且不让数据丢失
      */
    filteredData.persist(StorageLevel.MEMORY_AND_DISK)

    filteredData.collect().take(10).foreach(println(_))
    /**
      * 第二步：对原始的NBA球员数据进行基础性处理，基础数据项计算：方差、均值、最大值、最小值、出现次数等等；
      *
      */
    val itemStats: Array[String] = "FG,FGA,FG%,3P,3PA,3P%,2P,2PA,2P%,eFG%,FT,FTA,FT%,ORB,DRB,TRB,AST,STL,BLK,TOV,PF,PTS".split(",")
    itemStats.foreach(println)

    /**
      * 计算正则值的函数
      */

    def computeNomormalize(value : Double, max: Double, min: Double) = {
     value / math.max(math.abs(max), math.abs(min))
    }

    //保存球员基础信息、评分信息和正则化后的信息等；
    //x需要注意的是从statsZ开始的成员可能没有值，所以需要进行初始化，以防止运行时报错
    case class NBAPlayerData(year: Int, name: String, position: String, age: Int,team: String, gp:Int, gs:Int,
                             mp: Double, stats: Array[Double], statsZ: Array[Double] = Array[Double](), valueZ: Double = 0,
                             statsN: Array[Double] = Array[Double](), valueN:Double = 0, experience: Double = 0)


    /**
      * 把读取的每一行数据转换成为我们可以使用的数据NBAPalyerData的数据的方式
      *
      * (2001,Rk,Player,Pos,Age,Tm,G,GS,MP,FG,FGA,FG%,3P,3PA,3P,2P,
      * 2PA,2P,eFG%,FT,FTA,FT%,ORB,DRB,TRB,AST,STL,BLK,TOV,PF,PTS)
      */

    def rawData2NBAPalyerData(line: String, bStats : Map[String, Double] = Map.empty,zStats : Map[String, Double] = Map.empty) = {
      val content = line.replace(",,", ",0,") //进行数据清洗处理，把",,"换成",0,"
      val value = content.substring(1, content.length - 1).split(",")  //去掉了最左则和最右侧的括号,并且使用","切分开不同的数据项

      val year = value(0).toInt
      val name = value(2)
      val postion = value(3)
      val age = value(4).toInt
      val team = value(5)
      val gp = value(6).toInt
      val gs = value(7).toInt
      val mp = value(8).toDouble
      val stats = value.slice(9, 31).map(score => score.toDouble)
      var statsZ: Array[Double] = Array.empty
      var valueZ: Double = Double.NaN
      var statsN: Array[Double] = Array.empty
      var valueN: Double = Double.NaN

      /**
        * 在这里进行各种类型的正则化计算，并对前面的变量进行赋值；
        */
      if (!bStats.isEmpty) {
        val fg = (stats(2) - bStats.apply(year.toString + "_FG%_avg")) * stats(1)
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
          val fgN = computeNomormalize(zfg, (zStats.apply(year.toString + "_FG_max") - zStats.apply(year.toString + "_FG_avg"))
            / zStats.apply(year.toString + "_FG_stdev"), (zStats.apply(year.toString + "_FG_min")
            - zStats.apply(year.toString + "_FG_avg")) / zStats.apply(year.toString + "_FG_stdev"))
          val ftN = computeNomormalize(zft, (zStats.apply(year.toString + "_FT_max") - zStats.apply(year.toString + "_FT_avg"))
            / zStats.apply(year.toString + "_FT_stdev"), (zStats.apply(year.toString + "_FT_min")
            - zStats.apply(year.toString + "_FT_avg")) / zStats.apply(year.toString + "_FT_stdev"))
          val tpN = computeNomormalize(tp, zStats.apply(year.toString + "_3P_max"), zStats.apply(year.toString + "_3P_min"))
          val trbN = computeNomormalize(trb, zStats.apply(year.toString + "_TRB_max"), zStats.apply(year.toString + "_TRB_min"))
          val astN = computeNomormalize(ast, zStats.apply(year.toString + "_AST_max"), zStats.apply(year.toString + "_AST_min"))
          val stlN = computeNomormalize(stl, zStats.apply(year.toString + "_STL_max"), zStats.apply(year.toString + "_STL_min"))
          val blkN = computeNomormalize(blk, zStats.apply(year.toString + "_BLK_max"), zStats.apply(year.toString + "_BLK_min"))
          val tovN = computeNomormalize(tov, zStats.apply(year.toString + "_TOV_max"), zStats.apply(year.toString + "_TOV_min"))
          val ptsN = computeNomormalize(pts, zStats.apply(year.toString + "_PTS_max"), zStats.apply(year.toString + "_PTS_min"))
          statsZ = Array(zfg, zft, tp, trb, ast, stl, blk, tov, pts)
          valueZ = statsZ.reduce(_ + _)
          statsN = Array(fgN, ftN, tpN, trbN, astN, stlN, blkN, tovN, ptsN)
          valueN = statsN.reduce(_ + _)
        }
      }


      NBAPlayerData(year, name, postion,age, team,gp, gs, mp, stats, statsZ,valueZ, statsN,valueN)
    }


    /**
      * 接下来的步骤肯定是进行基础数据项的计算并且要进行广播，从而提升效率（一般而言，不变的小规模中间结果数据都是要进行广播的）
      */





    val basicData = computeStats(filteredData, itemStats).collect()

    val basicDataBroadcast = sc.broadcast(basicData)
    while(true){}
  }

  def computeStats(filteredData: RDD[String], itemStats: Array[String]):RDD[String] = {

    filteredData
  }
}
