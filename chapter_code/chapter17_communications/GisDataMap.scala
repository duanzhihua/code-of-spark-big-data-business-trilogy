package com.telecom.gis

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Spark商业案例书稿第17章： 光宽用户流量热力分布GIS应用Spark案例代码
  * 本代码需结合生产环境运行，代码供研读。
  * 版权：DT大数据梦工厂所有
  * 时间：2017年4月19日；
  * 数据格式：	PonPort, 宝山, 宝山宾馆北楼T1/HW5680T-OLT01, EPON 0/1/0, 201701302058-201701302210, 0, 0, 0.42321234, 0.063600122
  *
  ***/
object GisDataMap  {

  def main(args: Array[String]) {

    val warehouslocation =  "/spark-warehouse"
    val spark  = SparkSession.builder.appName("GisDataMap").enableHiveSupport().config("spark.sql.warehouse.dir", warehouslocation).getOrCreate()


    val schemaString2 = "OLTPORT2,FirstTime,LastTime,I_Mbps,IA_Mbps,O_Mbps,OA_Mbps"
    var fields2 = List[StructField]()
    for (columnName <- schemaString2.split(",")) {
      //分别规定String和Double类型
      if (columnName == "I_Mbps" || columnName == "IA_Mbps" || columnName == "O_Mbps" || columnName == "OA_Mbps" )
        fields2 = fields2 :+ StructField(columnName, DoubleType, true)
      else
        fields2 = fields2 :+ StructField(columnName, StringType, true)

    }
    val structType_ponData = StructType.apply(fields2)

    //结合数据  给出要处理的的时间 格式为yyyyMMdd
    val fileName =  "hdfs:///*/*/OltPonPort_FlowData_" + args(0) + "*"
    var ponData = spark.read.textFile(fileName).rdd


    //找出相关格式，进行关联
    val ponData_rdd = ponData.map(x => {
        val array = x.split(",")
        val port  = array(3).split("/")

        //使用\连接,一直特殊的地方去掉空格
        val oltPortTemp = array(2) + "|0" + port(1) + "|0" + port(2)
        val oltPort = oltPortTemp.replace(" ","")
        val firstTime = array(4).split("-")(0).replace(" ","")
        val lastTime = array(4).split("-")(1)

        Row(oltPort,firstTime,lastTime,array(5).toDouble,array(6).toDouble,array(7).toDouble,array(8).toDouble)

      })

    var df_ponData = spark.createDataFrame(ponData_rdd, structType_ponData)

    //在此读hive读取相关的表，进行定期操作
    val df_gisData = spark.sql("SELECT * FROM OLTPORT_LOCATION")



      //进行关联
    val result = df_ponData.join(df_gisData, df_ponData.col("OLTPORT2") === df_gisData.col("olt_port")).drop("OLTPORT2")

    result.show(20)

    val printRdd = result.rdd.map(x => {

      var result  = x.apply(0)
      for(i <- 1 to x.length - 1){
        result = result + "," + x.apply(i)
      }
      result
    })


    //针对针对以上进行处理
    //进行再一次清洗,给出JD WD

    result.foreachPartition(partition => {


      //分区域 进行数据库连接,减少连接池的压力
      val url = "jdbc:oracle:thin:@10.*.*.*:1521:ORCL"
      val user = ""
      val password = ""

      var conn: Connection = null
      var ps: PreparedStatement = null

      //插入数据库语句
      val sql = "insert into ogg.DX_LLXXB(xh,firsttime,lasttime,i_mbps,ia_mbps,o_mbps,oa_mbps,oltport,olmc,l_local,jd,wd,drrq,ywrq,jd1,wd1) values(SEQ_DXLL.nextval,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

      try {

        //给出链接
        conn = DriverManager.getConnection(url, user, password)
        conn.setAutoCommit(false)
        //拼接SQL
        ps = conn.prepareStatement(sql)

        //每个分区内部批处理
        partition.toList.foreach( x => {
          //给出经纬度计算
          val jd = x.getAs[String]("ad_location").split(":")(0)
          val wd = x.getAs[String]("ad_location").split(":")(1)

          //转换后的经纬度
          val jd1 = jd.toDouble * 20037508.34 / 180
          var wd1 = math.log(Math.tan((90 + wd.toDouble) * math.Pi / 360)) / (math.Pi / 180)
          wd1 = wd1 * 20037508.34 / 180

          //当前的时间
          //给出当前时间
          var now = new Date()
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val DRRQ = sdf.format(now)
          val YWRQ = x.getAs[String]("FirstTime")

          //对SQL进行赋值
          ps.setString(1,x.getAs[String]("FirstTime"))
          ps.setString(2,x.getAs[String]("LastTime"))
          ps.setDouble(3,x.getAs[Double]("I_Mbps"))
          ps.setDouble(4,x.getAs[Double]("IA_Mbps"))
          ps.setDouble(5,x.getAs[Double]("O_Mbps"))
          ps.setDouble(6,x.getAs[Double]("OA_Mbps"))
          ps.setString(7,x.getAs[String]("olt_port"))

          //OLT 的名称 OLTPORT的第一个/前的名称
          ps.setString(8,x.getAs[String]("olt_port").split("/")(0))

          ps.setString(9,x.getAs[String]("ad_location"))
          ps.setString(10,jd)
          ps.setString(11,wd)
          ps.setString(12,DRRQ)
          ps.setString(13,YWRQ)

          //进行格式的规范化，保留7位小数
          val df = new DecimalFormat("#.0000000")

          ps.setString(14,df.format(jd1))
          ps.setString(15,df.format(wd1))

          //进行批处理
          ps.addBatch()
        })

        //进行批处理
        ps.executeBatch()
        conn.commit()
      }// end try
      catch {
        case e:Exception => e.printStackTrace
      }

      finally
      {
        //关闭两个管道
        if (ps != null) {
          ps.close()
        }

        if (conn != null) {
          conn.close()
        }
      }



    })



  }
}
