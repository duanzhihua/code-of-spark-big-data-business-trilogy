package com.dt.spark.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
/**
  * Spark商业案例书稿第七章：7.19 婚恋社交网络多维度分析案例Spark GraphX图操作的案例代码
  * 版权：DT大数据梦工厂所有
  * 时间：2017年4月19日；
  *
  ***/

object Graphx_webGoogle {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[8]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("Graphx_webGoogle")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext
    //数据存放的目录；
    var dataPath = "data/web-Google/"
    val graphFromFile: Graph[PartitionID, PartitionID] = GraphLoader.edgeListFile(sc, dataPath + "web-Google.txt", numEdgePartitions = 4)
    //统计顶点的数量
    println("graphFromFile.vertices.count：    " + graphFromFile.vertices.count())
    //统计边的数量
    println("graphFromFile.edges.count：    " + graphFromFile.edges.count())

    val subGraph: Graph[PartitionID, PartitionID] =graphFromFile.subgraph(epred = e  => e.srcId > e.dstId)
    for(elem <- subGraph.edges.take(10)) {
      println("subGraph.edges：    "+  elem)
    }

    println("subGraph.vertices.count()：    "+  subGraph.vertices.count())
    println("subGraph.edges.count()：    "+  subGraph.edges.count())

    val subGraph2: Graph[PartitionID, PartitionID] = graphFromFile.subgraph(epred = e  => e.srcId > e.dstId,vpred= (id, _) => id>1000000)
    println("subGraph2.vertices.count()：    "+  subGraph2.vertices.count())
    println("subGraph2.edges.count()：    "+  subGraph2.edges.count())

    val tmp: VertexRDD[PartitionID] =graphFromFile.inDegrees
    for (elem <- tmp.take(10)) {
      println("graphFromFile.inDegrees：    " + elem)
    }
    val tmp1: VertexRDD[PartitionID] =graphFromFile.outDegrees
    for (elem <- tmp1.take(10)) {
      println("graphFromFile.outDegrees：    " + elem)
    }

    val tmp2: VertexRDD[PartitionID] =graphFromFile.degrees
    for (elem <- tmp2.take(10)) {
      println("graphFromFile.degrees：    " + elem)
    }

    def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int)=if(a._2 > b._2) a else b
    println("graphFromFile.degrees.reduce(max)：    " + graphFromFile.degrees.reduce(max) )
    println("graphFromFile.inDegrees.reduce(max)：    " + graphFromFile.inDegrees.reduce(max) )
    println("graphFromFile.outDegrees.reduce(max)：    " + graphFromFile.outDegrees.reduce(max) )


    val rawGraph: Graph[PartitionID, PartitionID] =graphFromFile.mapVertices((id, attr) =>0 )
    for (elem <- rawGraph.vertices.take(10)) {
      println("rawGraph.vertices：    " + elem)
    }

    val outDeg: VertexRDD[PartitionID] =rawGraph.outDegrees

    val tmpJoinVertices: Graph[PartitionID, PartitionID] =rawGraph.joinVertices[Int](outDeg)((_, _, optDeg) => optDeg)
    for (elem <- tmpJoinVertices.vertices.take(10)) {
      println("tmpJoinVertices.vertices：    " + elem)
    }

    val tmpouterJoinVertices: Graph[PartitionID, PartitionID] =rawGraph.outerJoinVertices[Int,Int](outDeg)((_, _, optDeg) =>optDeg.getOrElse(0))
    for (elem <- tmpouterJoinVertices.vertices.take(10)) {
      println("tmpouterJoinVertices.vertices：    " + elem)
    }


    for (elem <- graphFromFile.vertices.take(10)) {
      println("graphFromFile.vertices：    " + elem)
    }

    val tmpGraph: Graph[PartitionID, PartitionID] =graphFromFile.mapVertices((vid, attr) => attr.toInt*2)
    for (elem <- tmpGraph.vertices.take(10)) {
      println("tmpGraph.vertices：    " + elem)
    }

    //Pregel API Example
    val sourceId: VertexId = 0
    val g: Graph[Double, PartitionID] =graphFromFile.mapVertices((id, _) =>if(id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp: Graph[Double, PartitionID] = g.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println("sssp.vertices.collect:  "+ sssp.vertices.collect.take(10).mkString("\n"))
    val rank: VertexRDD[Double] = graphFromFile.pageRank(0.01).vertices
    for (elem <- rank.take(10)) {
      println("rank：    " + elem)
    }

    val graphFortriangleCount: Graph[PartitionID, PartitionID] = GraphLoader.edgeListFile(sc, dataPath + "web-Google.txt", true)
    val c: VertexRDD[PartitionID] = graphFromFile.triangleCount().vertices
    for (elem <- c.take(10)) {
      println("triangleCount：    " + elem)
    }

  //  while (true) {}
  }
}
