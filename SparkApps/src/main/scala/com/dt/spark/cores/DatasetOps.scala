package com.dt.spark.cores

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.collection.mutable.ArrayBuffer
/**
  *  Spark商业案例书稿第二章：2.11 Dataset开发实战企业人员管理系统应用案例代码
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月1日；
  **/
object DatasetOps {

  case class Person(name: String, age: Long)

  case class Score(n: String, score: Long)


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("DatasetOps").master("local[4]")
      .config("spark.sql.warehouse.dir", "G:\\IMFBigDataSpark2017\\SparkApps\\spark-warehouse")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    /**
      * Dataset中的tranformation和Action操作，Action类型的操作有：
      * show collect first reduce take count等
      * 这些操作都会产生结果，也就是说会执行逻辑计算的过程
      */

    val personsDF = spark.read.json("data/peoplemanagedata/people.json")
    val personScoresDF = spark.read.json("data/peoplemanagedata/peopleScores.json")

    val personsDS = personsDF.as[Person]
    val personScoresDS = personScoresDF.as[Score]

    println("使用groupBy算子进行分组：")
    val personsDSGrouped = personsDS.groupBy($"name", $"age").count()
    personsDSGrouped.show()

    println("使用agg算子concat内置函数将姓名、年龄连接在一起成为单个字符串列 ：")
    personsDS.groupBy($"name", $"age").agg(concat($"name", $"age")).show

    println("使用col算子选择列 ：")
    personsDS.joinWith(personScoresDS, personsDS.col("name") === personScoresDS.col("n")).show

    println("使用sum、avg等函数计数年龄总和、平均年龄、最大年龄、最小年龄、唯一年龄计数、平均年龄、当前时间等数据 ：")
    personsDS.groupBy($"name").agg(sum($"age"), avg($"age"), max($"age"), min($"age"), count($"age")
      , countDistinct($"age"), mean($"age"), current_date()).show

    println("函数collect_list、collect_set比较，collect_list函数结果中包含重复元素；collect_set函数结果中无重复元素：")
    personsDS.groupBy($"name")
      .agg(collect_list($"name"), collect_set($"name"))
      .show()

    println("使用sample算子进行随机采样：")
    personsDS.sample(false, 0.5).show()

    println("使用randomSplit算子进行随机切分：")
    personsDS.randomSplit(Array(10, 20)).foreach(dataset => dataset.show())
    println("使用select算子选择列：")
    personsDS.select("name").show()

    println("使用joinWith算子关联企业人员信息、企业人员分数评分信息：")
    personsDS.joinWith(personScoresDS, $"name" === $"n").show

    println("使用join算子关联企业人员信息、企业人员分数评分信息：")
    personsDS.join(personScoresDS, $"name" === $"n").show

    println("使用sort算子对年龄进行降序排序：")
    personsDS.sort($"age".desc).show

    import spark.implicits._

    def myFlatMapFunction(myPerson: Person, myEncoder: Person): Dataset[Person] = {
      personsDS
    }

    personsDS.flatMap(persons => persons match {
      case Person(name, age) if (name == "Andy") => List((name, age + 70))
      case Person(name, age) => List((name, age + 30))
    }).show()

    personsDS.mapPartitions { persons =>
      val result = ArrayBuffer[(String, Long)]()
      while (persons.hasNext) {
        val person = persons.next()
        result += ((person.name, person.age + 1000))
      }
      result.iterator

    }.show

    println("使用dropDuplicates算子统计企业人员管理系统姓名无重复员工的记录：")
    personsDS.dropDuplicates("name").show()
    personsDS.distinct().show()

    println("使用repartition算子设置分区：")
    println("原分区数：" + personsDS.rdd.partitions.size)
    val repartitionDs = personsDS.repartition(4)
    println("repartition设置分区数：" + repartitionDs.rdd.partitions.size)

    println("使用coalesce算子设置分区：")
    val coalesced: Dataset[Person] = repartitionDs.coalesce(2)
    println("coalesce设置分区数：" + coalesced.rdd.partitions.size)
    coalesced.show

    spark.stop()

  }
}

