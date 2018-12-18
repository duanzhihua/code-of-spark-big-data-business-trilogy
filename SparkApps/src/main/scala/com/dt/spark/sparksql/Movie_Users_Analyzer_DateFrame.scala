package com.dt.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.immutable.HashSet




/**
  *  Spark商业案例书稿第一章：1.12.3.2 DateFrame方式案例代码
  *  版权：DT大数据梦工厂所有
  * 时间：2017年1月11日；
  * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
  *   数据采集：企业中一般越来越多的喜欢直接把Server中的数据发送给Kafka，因为更加具备实时性；
  *   数据过滤：趋势是直接在Server端进行数据过滤和格式化，当然采用Spark SQL进行数据的过滤也是一种主要形式；
  *   数据处理：
  *     1，一个基本的技巧是，先使用传统的SQL去实现一个下数据处理的业务逻辑（自己可以手动模拟一些数据）；
  *     2，再一次推荐使用DataSet去实现业务功能尤其是统计分析功能；
  *     3，如果你想成为专家级别的顶级Spark人才，请使用RDD实现业务功能，为什么？运行的时候是基于RDD的！
  *
  *  数据：强烈建议大家使用Parquet
  *  1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
  *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
  *  3，"movies.dat"：MovieID::Title::Genres
  *  4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
  */
object Movie_Users_Analyzer_DateFrame {
  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[8]" //默认程序运行在本地Local模式中，主要学习和测试；
    var dataPath = "data/moviedata/medium/"  //数据存放的目录；

    /**
      * 当我们把程序打包运行在集群上的时候一般都会传入集群的URL信息，在这里我们假设如果传入
      * 参数的话，第一个参数只传入Spark集群的URL第二个参数传入的是数据的地址信息；
      */
    if(args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }


    /**
      * 创建Spark会话上下文SparkSession和集群上下文SparkContext，在SparkConf中可以进行各种依赖和参数的设置等，
      * 大家可以通过SparkSubmit脚本的help去看设置信息，其中SparkSession统一了Spark SQL运行的不同环境。
      */
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("Movie_Users_Analyzer_SparkSQL")

    /**
      * SparkSession统一了Spark SQL执行时候的不同的上下文环境，也就是说Spark SQL无论运行在那种环境下我们都可以只使用
      * SparkSession这样一个统一的编程入口来处理DataFrame和DataSet编程，不需要关注底层是否有Hive等。
      */
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext //从SparkSession获得的上下文，这是因为我们读原生文件的时候或者实现一些Spark SQL目前还不支持的功能的时候需要使用SparkContext


    /**
      * 读取数据，用什么方式读取数据呢？在这里是使用RDD!
      */
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")


    /**
      * 功能一：通过DataFrame实现某特定电影观看者中男性和女性不同年龄分别有多少人？
      *   1，从点评数据中获得观看者的信息ID；
      *   2，把ratings和users表进行join操作获得用户的性别信息；
      *   3，使用内置函数（内部包含超过200个内置函数）进行信息统计和分析
      *  在这里我们通过DataFrame来实现：首先通过DataFrame的方式来表现ratings和users的数据，然后进行join和统计操作
      */

    println("功能一：通过DataFrame实现某特定电影观看者中男性和女性不同年龄分别有多少人？")
    val schemaforusers: StructType = StructType("UserID::Gender::Age::OccupationID::Zip-code".split("::").
      map(column => StructField(column, StringType, true))) //使用Struct方式把Users的数据格式化,即在RDD的基础上增加数据的元数据信息
    val usersRDDRows: RDD[Row] = usersRDD.map(_.split("::")).map(line => Row(line(0).trim,line(1).
      trim,line(2).trim,line(3).trim,line(4).trim)) //把我们的每一条数据变成以Row为单位的数据
    val usersDataFrame: DataFrame = spark.createDataFrame(usersRDDRows, schemaforusers)  //结合Row和StructType的元数据信息基于RDD创建DataFrame，这个时候RDD就有了元数据信息的描述

    val schemaforratings = StructType("UserID::MovieID".split("::").
      map(column => StructField(column, StringType, true))).
      add("Rating", DoubleType, true).
      add("Timestamp",StringType, true)

    val ratingsRDDRows: RDD[Row] = ratingsRDD.map(_.split("::")).map(line => Row(line(0).trim,line(1).
      trim,line(2).trim.toDouble,line(3).trim))
    val ratingsDataFrame: DataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)

    val schemaformovies = StructType("MovieID::Title::Genres".split("::").
      map(column => StructField(column, StringType, true))) //使用Struct方式把Users的数据格式化,即在RDD的基础上增加数据的元数据信息
    val moviesRDDRows = moviesRDD.map(_.split("::")).map(line => Row(line(0).trim,line(1).
      trim,line(2).trim)) //把我们的每一条数据变成以Row为单位的数据
    val moviesDataFrame: DataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)  //结合Row和StructType的元数据信息基于RDD创建DataFrame，这个时候RDD就有了元数据信息的描述


    ratingsDataFrame.filter(s" MovieID = 1193") //这里能够直接指定MovieID的原因是DataFrame中有该元数据信息！
          .join(usersDataFrame, "UserID") //Join的时候直接指定基于UserID进行Join，这相对于原生的RDD操作而言更加方便快捷
          .select("Gender", "Age")  //直接通过元数据信息中的Gender和Age进行数据的筛选
          .groupBy("Gender", "Age") //直接通过元数据信息中的Gender和Age进行数据的groupBy操作
          .count()  //基于groupBy分组信息进行count统计操作
          .show(10) //显示出分组统计后的前10条信息
    /**
      * 功能二：用SQL语句实现某特定电影观看者中男性和女性不同年龄分别有多少人？
      * 1，注册临时表，写SQL语句需要Table；
      * 2，基于上述注册的零时表写SQL语句；
      */
    println("功能二：用GlobalTempView的SQL语句实现某特定电影观看者中男性和女性不同年龄分别有多少人？")
    ratingsDataFrame.createGlobalTempView("ratings")
    usersDataFrame.createGlobalTempView("users")


    spark.sql("SELECT Gender, Age, count(*) from  global_temp.users u join  global_temp.ratings as r on u.UserID = r.UserID where MovieID = 1193" +
      " group by Gender, Age").show(10)

    println("功能二：用LocalTempView的SQL语句实现某特定电影观看者中男性和女性不同年龄分别有多少人？")
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView("users")


    spark.sql("SELECT Gender, Age, count(*) from  users u join  ratings as r on u.UserID = r.UserID where MovieID = 1193" +
      " group by Gender, Age").show(10)

    /**
      * 功能三：使用DataFrame进行电影流行度分析：所有电影中平均得分最高（口碑最好）的电影及观看人数最高的电影（流行度最高）
      * "ratings.dat"：UserID::MovieID::Rating::Timestamp
      * 得分最高的Top10电影实现思路：如果想算总的评分的话一般肯定需要reduceByKey操作或者aggregateByKey操作
      *   第一步：把数据变成Key-Value，大家想一下在这里什么是Key，什么是Value。把MovieID设置成为Key，把Rating设置为Value；
      *   第二步：通过reduceByKey操作或者aggregateByKey实现聚合，然后呢？
      *   第三步：排序，如何做？进行Key和Value的交换
      */
    println("纯粹通过RDD的方式实现所有电影中平均得分最高（口碑最好）的电影TopN:")
    val ratings= ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2))).cache()  //格式化出电影ID和评分
    ratings.map(x => (x._2, (x._3.toDouble, 1.toDouble)))  //格式化成为Key-Value的方式
      .reduceByKey((x, y) => (x._1 + y._1,x._2 + y._2)) //对Value进行reduce操作，分别得出每部电影的总的评分和总的点评人数
      .map(x => (x._2._1.toDouble / x._2._2.toDouble, x._1))  //求出电影平均分
      .sortByKey(false) //降序排序
        .map(x => (x._2, x._1))
      .take(10) //取Top10
      .foreach(println) //打印到控制台
    val aa: RDD[Row] =ratingsDataFrame.select("MovieID", "Rating").groupBy("MovieID").
      avg("Rating").rdd
    print("==============teset=====20170302========")
    ratingsDataFrame.printSchema()
    aa.collect().take(10).foreach(println)

    println("通过DataFrame和RDD相结合的方式计算所有电影中平均得分最高（口碑最好）的电影TopN:")
    ratingsDataFrame.select("MovieID", "Rating").groupBy("MovieID").
      avg("Rating").rdd.
      map(row => (row(1),(row(0), row(1)))).sortBy(_._1.toString.toDouble, false).
      map(tuple => tuple._2).collect.take(10).foreach(println)

    import spark.sqlContext.implicits._
    println("通过纯粹使用DataFrame方式计算所有电影中平均得分最高（口碑最好）的电影TopN:")
    ratingsDataFrame.select("MovieID", "Rating").groupBy("MovieID").
      avg("Rating").orderBy($"avg(Rating)".desc).show(10)
    /**
      * 上面的功能计算的是口碑最好的电影，接下来我们分析粉丝或者观看人数最多的电影
      */
    println("纯粹通过RDD的方式计算所有电影中粉丝或者观看人数最多(最流行电影)的电影TopN:")
    ratings.map(x => (x._2, 1)).reduceByKey(_+_).map(x => (x._2, x._1)).
      sortByKey(false).map(x => (x._2, x._1)).collect()
      .take(10).foreach(println)
    println("通过DataFrame和RDD结合的方式计算最流行电影即所有电影中粉丝或者观看人数最多(最流行电影)的电影TopN:")
    ratingsDataFrame.select("MovieID","Timestamp").groupBy("MovieID").count().rdd.
      map(row => (row(1).toString.toLong, (row(0), row(1)))).sortByKey(false).
      map(tuple => tuple._2).collect().take(10).foreach(println)

    println("纯粹通过DataFrame的方式计算最流行电影即所有电影中粉丝或者观看人数最多(最流行电影)的电影TopN:")
//    ratingsDataFrame.select("MovieID","Timestamp").
//    ratingsDataFrame.select("MovieID").
      ratingsDataFrame.groupBy("MovieID").count().
      orderBy($"count".desc).show(10)


    /**
      * 功能四：分析最受男性喜爱的电影Top10和最受女性喜爱的电影Top10
      * 1，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
      * 2，"ratings.dat"：UserID::MovieID::Rating::Timestamp
      *   分析：单单从ratings中无法计算出最受男性或者女性喜爱的电影Top10,因为该RDD中没有Gender信息，如果我们需要使用
      *     Gender信息来进行Gender的分类，此时一定需要聚合，当然我们力求聚合的使用是mapjoin（分布式计算的Killer
      *     是数据倾斜，map端的join是一定不会数据倾斜），在这里可否使用mapjoin呢？不可以，因为用户的数据非常多！
      *     所以在这里要使用正常的Join，此处的场景不会数据倾斜，因为用户一般都很均匀的分布（但是系统信息搜集端要注意黑客攻击）
      *
      * Tips：
      *   1，因为要再次使用电影数据的RDD，所以复用了前面Cache的ratings数据
      *   2, 在根据性别过滤出数据后关于TopN部分的代码直接复用前面的代码就行了。
      *   3, 要进行join的话需要key-value；
      *   4, 在进行join的时候时刻通过take等方法注意join后的数据格式  (3319,((3319,50,4.5),F))
      *   5, 使用数据冗余来实现代码复用或者更高效的运行，这是企业级项目的一个非常重要的技巧！
      */
    val male = "M"
    val female = "F"
    val genderRatings = ratings.map(x => (x._1, (x._1, x._2, x._3))).join(
      usersRDD.map(_.split("::")).map(x => (x(0), x(1)))).cache()
    val genderRatingsDataFrame: DataFrame = ratingsDataFrame.join(usersDataFrame, "UserID").cache()

    genderRatingsDataFrame.show(10)
    genderRatings.take(10).foreach(println)
    val maleFilteredRatings = genderRatings.filter(x => x._2._2.equals("M")).map(x => x._2._1)
    val maleFilteredRatingsDataFrame = genderRatingsDataFrame.filter("Gender= 'M'").select("MovieID", "Rating")
    val femaleFilteredRatings = genderRatings.filter(x => x._2._2.equals("F")).map(x => x._2._1)
    val femaleFilteredRatingsDataFrame = genderRatingsDataFrame.filter("Gender= 'F'").select("MovieID", "Rating")

    /**
      * (855,5.0)
        (6075,5.0)
        (1166,5.0)
        (3641,5.0)
        (1045,5.0)
        (4136,5.0)
        (2538,5.0)
        (7227,5.0)
        (8484,5.0)
        (5599,5.0)
      */
    println("纯粹使用RDD实现所有电影中最受男性喜爱的电影Top10:")
    maleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))  //格式化成为Key-Value的方式
      .reduceByKey((x, y) => (x._1 + y._1,x._2 + y._2)) //对Value进行reduce操作，分别得出每部电影的总的评分和总的点评人数
      .map(x => (x._2._1.toDouble / x._2._2, x._1))  //求出电影平均分
      .sortByKey(false) //降序排序
      .map(x => (x._2, x._1))
      .take(10) //取Top10
      .foreach(println) //打印到控制台
    println("纯粹使用DataFrame实现所有电影中最受男性喜爱的电影Top10:")
    maleFilteredRatingsDataFrame.groupBy("MovieID").avg("Rating").orderBy($"avg(Rating)".desc).show(10)
    /**
      * (789,5.0)
        (855,5.0)
        (32153,5.0)
        (4763,5.0)
        (26246,5.0)
        (2332,5.0)
        (503,5.0)
        (4925,5.0)
        (8767,5.0)
        (44657,5.0)
      */
    println("纯粹使用RDD实现所有电影中最受女性喜爱的电影Top10:")
    femaleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))  //格式化成为Key-Value的方式
      .reduceByKey((x, y) => (x._1 + y._1,x._2 + y._2)) //对Value进行reduce操作，分别得出每部电影的总的评分和总的点评人数
      .map(x => (x._2._1.toDouble / x._2._2, x._1))  //求出电影平均分
      .sortByKey(false) //降序排序
      .map(x => (x._2, x._1))
      .take(10) //取Top10
      .foreach(println) //打印到控制台
    println("纯粹使用DataFrame实现所有电影中最受女性喜爱的电影Top10:")
    femaleFilteredRatingsDataFrame.groupBy("MovieID").avg("Rating").orderBy($"avg(Rating)".desc, $"MovieID".desc).show(10)

    /**
      * 思考题：如果想让RDD和DataFrame计算的TopN的每次结果都一样，该如何保证？现在的情况是例如计算Top10，而其同样评分的不止10个，所以每次都会
      * 从中取出10个，这就导致的大家的结果不一致，这个时候，我们可以使用一个新的列参与排序：
      *   如果是RDD的话，该怎么做呢？这个时候就要进行二次排序，按照我们前面和大家讲解的二次排序的视频内容即可。
      *   如果是DataFrame的话，该如何做呢？此时就非常简单，我们只需要再orderBy函数中增加一个排序维度的字段即可，简单的不可思议！
      */

    /**
      * 功能五：最受不同年龄段人员欢迎的电影TopN
      * "users.dat"：UserID::Gender::Age::OccupationID::Zip-code
      * 思路：首先还是计算TopN，但是这里的关注点有两个：
      *   1，不同年龄阶段如何界定，关于这个问题其实是业务的问题，当然，你实际在实现的时候可以使用RDD的filter中的例如
      *     13 < age <18,这样做会导致运行时候大量的计算，因为要进行扫描，所以会非常耗性能。所以，一般情况下，我们都是
      *     在原始数据中直接对要进行分组的年龄段提前进行好ETL, 例如进行ETL后产生以下的数据：
      *     - Gender is denoted by a "M" for male and "F" for female
      *     - Age is chosen from the following ranges:
      *  1:  "Under 18"
      * 18:  "18-24"
      * 25:  "25-34"
      * 35:  "35-44"
      * 45:  "45-49"
      * 50:  "50-55"
      * 56:  "56+"
      *   2，性能问题：
      *     第一点：你实际在实现的时候可以使用RDD的filter中的例如13 < age <18,这样做会导致运行时候大量的计算，因为要进行
      *     扫描，所以会非常耗性能，我们通过提前的ETL把计算发生在Spark业务逻辑运行以前，用空间换时间，当然这些实现也可以
      *     使用Hive，因为Hive语法支持非常强悍且内置了最多的函数；
      *     第二点：在这里要使用mapjoin，原因是targetUsers数据只有UserID，数据量一般不会太多
      */


    val targetQQUsers = usersRDD.map(_.split("::")).map(x => (x(0), x(2))).filter(_._2.equals("18"))
    val targetTaobaoUsers = usersRDD.map(_.split("::")).map(x => (x(0), x(2))).filter(_._2.equals("25"))

    /**
      * 在Spark中如何实现mapjoin呢，显然是要借助于Broadcast，会把数据广播到Executor级别让该Executor上的所有任务共享
      *  该唯一的数据，而不是每次运行Task的时候都要发送一份数据的拷贝，这显著的降低了网络数据的传输和JVM内存的消耗
      */
    val targetQQUsersSet = HashSet() ++ targetQQUsers.map(_._1).collect()
    val targetTaobaoUsersSet = HashSet() ++ targetTaobaoUsers.map(_._1).collect()

    val targetQQUsersBroadcast = sc.broadcast(targetQQUsersSet)
    val targetTaobaoUsersBroadcast = sc.broadcast(targetTaobaoUsersSet)

    /**
      * QQ或者微信核心目标用户最喜爱电影TopN分析
      * (Silence of the Lambs, The (1991),524)
        (Pulp Fiction (1994),513)
        (Forrest Gump (1994),508)
        (Jurassic Park (1993),465)
        (Shawshank Redemption, The (1994),437)
      */
    val movieID2Name = moviesRDD.map(_.split("::")).map(x => (x(0), x(1))).collect.toMap
    println("纯粹通过RDD的方式实现所有电影中QQ或者微信核心目标用户最喜爱电影TopN分析:")
    ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(x =>
      targetQQUsersBroadcast.value.contains(x._1)
    ).map(x => (x._2, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).
      sortByKey(false).map(x => (x._2, x._1)).take(10).
      map(x => (movieID2Name.getOrElse(x._1, null), x._2)).foreach(println)

    println("纯粹通过DataFrame的方式实现所有电影中QQ或者微信核心目标用户最喜爱电影TopN分析:")

    ratingsDataFrame.join(usersDataFrame, "UserID").filter("Age = '18'").groupBy("MovieID").
      count().orderBy($"count".desc).printSchema()

    /**
      * Tips:
      *   1,orderBy操作需要在join之后进行
      */
    ratingsDataFrame.join(usersDataFrame, "UserID").filter("Age = '18'").groupBy("MovieID").
      count().join(moviesDataFrame, "MovieID").select("Title", "count").orderBy($"count".desc).show(10)




    /**
      * 淘宝核心目标用户最喜爱电影TopN分析
      * (Pulp Fiction (1994),959)
        (Silence of the Lambs, The (1991),949)
        (Forrest Gump (1994),935)
        (Jurassic Park (1993),894)
        (Shawshank Redemption, The (1994),859)
      */
    println("纯粹通过RDD的方式实现所有电影中淘宝核心目标用户最喜爱电影TopN分析:")
    ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(x =>
      targetTaobaoUsersBroadcast.value.contains(x._1)
    ).map(x => (x._2, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).
      sortByKey(false).map(x => (x._2, x._1)).take(10).
      map(x => (movieID2Name.getOrElse(x._1, null), x._2)).foreach(println)

    println("纯粹通过DataFrame的方式实现所有电影中淘宝核心目标用户最喜爱电影TopN分析:")
    ratingsDataFrame.join(usersDataFrame, "UserID").filter("Age = '25'").groupBy("MovieID").
      count().join(moviesDataFrame, "MovieID").select("Title", "count").orderBy($"count".desc).show(10)




       while(true){} //和通过Spark shell运行代码可以一直看到Web终端的原理是一样的，因为Spark Shell内部有一个LOOP循环

    sc.stop()


  }
}
