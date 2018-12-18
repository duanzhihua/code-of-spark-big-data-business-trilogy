package com.dt.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
//  在图计算的例子运行中我们需要导入RDD的包
import org.apache.spark.rdd.RDD

/**
  *  Spark商业案例书稿第七章：7.6 实战第一个Graph代码实例并进行Vertices、edges、triplets操作
  *  版权：DT大数据梦工厂所有
  * 时间：2017年1月11日；
  * */
object Graphx_VerticesEdgesTriplets {
  def main(args: Array[String]) {


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
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("Graphx_VerticesEdgesTriplets")

    /**
      * SparkSession统一了Spark SQL执行时候的不同的上下文环境，也就是说Spark SQL无论运行在那种环境下我们都可以只使用
      * SparkSession这样一个统一的编程入口来处理DataFrame和DataSet编程，不需要关注底层是否有Hive等。
      */
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext //从SparkSession获得的上下文，这是因为我们读原生文件的时候或者实现一些Spark SQL目前还不支持的功能的时候需要使用SparkContext

    // 创建顶点的RDD
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // 创建边的RDD
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // 定义案例中顶点的默认使用者，在缺失使用者的情况，图可以建立关联
    val defaultUser = ("John Doe", "Missing")
    // 初始化图
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    // 统计图中职业是博士后的节点
    val filteredVertices: VertexId = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    System.out.println("图中职业是博士后的节点数量计数为：    " + filteredVertices)
    // 统计图中边的源ID大于目的ID
    val filteredSrcDstId: VertexId = graph.edges.filter(e => e.srcId > e.dstId).count
    System.out.println("图中边的源ID大于目的ID的数量是：    " + filteredSrcDstId)

    val resTriplets: RDD[EdgeTriplet[(String, String), String]] = graph.triplets
    for (elem <- resTriplets.collect()) {
      println("三元组：    " + elem)
    }

    for (elem <- graph.vertices.collect()) {
      println("graph.vertices：    " + elem)
    }

    for (elem <- graph.edges.collect()) {
      println("graph.edges：    " + elem)
    }

    //数据存放的目录；
  var dataPath = "data/web-Google/"

    val graphFromFile: Graph[PartitionID, PartitionID] = GraphLoader.edgeListFile(sc, dataPath + "web-Google.txt")
    for (elem <- graphFromFile.vertices.collect()) {
      println("graphFromFile.vertices：    " + elem)
    }

  //  while (true) {}


  }

}
