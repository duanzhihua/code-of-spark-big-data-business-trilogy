package com.dt.spark.IMF114;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.function_return;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

public class SparkSQLUserlogsHottest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLUserlogsHottest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new HiveContext(sc);

		JavaRDD<String> lines0 = sc.textFile("G:\\IMFBigDataSpark2016\\tesdata\\SparkSQLUserlogsHottest\\SparkSQLUserlogsHottest0422.log");
		 
		/*元数据：Date、UserID、Item、City、Device；
          (date#Item#userID)
		*/
				
		//广播变量定义好了
		  String  devicebd ="iphone";
		  final Broadcast<String>   broadcastdevice =sc.broadcast(devicebd);	 
		// 过滤 一下吧
		
		// lines.filter();
		  JavaRDD<String> lines =lines0.filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String s) throws Exception {
				return s.contains(broadcastdevice.value());
			}
		}); 
		  
	 	  // 打印验证已经过滤了
		  List<String>  listRow000 = lines.collect();
					for(  String row : listRow000){
						System.out.println(row);
					}
      
		  
		 //组拼字符串(date#Item#userID)  构建KV(date#Item#userID，1)
		 JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
		        private static final long serialVersionUID =1L ;		        
		        @Override					
				public Tuple2<String, Integer> call(String line) throws Exception {
					String[] splitedLine =line.split("\t");
			        int  one = 1; 
					String dataanditemanduserid = splitedLine[0] +"#"+ splitedLine[2]+"#"+String.valueOf(splitedLine[1]);
					return new Tuple2<String,Integer>(String.valueOf(dataanditemanduserid),Integer.valueOf(one));
					
				}
			});
		 
		 // 打印验证一下
		    List<Tuple2<String,Integer>>  listRow = pairs.collect();
					for(Tuple2<String,Integer> row : listRow){
						System.out.println(row._1);
						System.out.println(row._2);
					}
        
				    
			 //reducebykey，统计计数
					
	       JavaPairRDD<String, Integer>  reduceedPairs =pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2 ;
			}
		});
				    	 
				    
	       List<Tuple2<String,Integer>>  reduceedRow = reduceedPairs.collect();
	       
	   	    //动态组拼出JSON
			List<String> userItemInformations = new ArrayList<String>();
			
			
			for(Tuple2<String,Integer> row : reduceedRow){
				//拆分三个字段
			    String[] rowSplitedLine =row._1.split("#");
				String rowuserID = rowSplitedLine[2];
				String rowitemID = rowSplitedLine[1];
				String rowdateID = rowSplitedLine[0];
			
			//拼接json  元数据：Date、UserID、Item、City、Device	
				String jsonzip=	"{\"Date\":\""+ rowdateID  
				             +"\", \"UserID\":\""+ rowuserID  
				             +"\", \"Item\":\""+ rowitemID  
				             +"\", \"count\":"+ row._2 +" }";
  			     userItemInformations.add(jsonzip);
				
			}	 
				          
			//打印验证 
		 			
			for(String row : userItemInformations){
				System.out.println(row.toString());
				//System.out.println(row._2);
			}
			
			//通过内容为JSON的RDD来构造DataFrame
			JavaRDD<String> userItemInformationsRDD = sc.parallelize(userItemInformations);
			DataFrame userItemInformationsDF = sqlContext.read().json(userItemInformationsRDD);
			
			userItemInformationsDF.show();
			
			//注册成为临时表
			userItemInformationsDF.registerTempTable("userItemInformations");
				
			
			  /* 使用子查询的方式完成目标数据的提取，在目标数据内幕使用窗口函数row_number来进行分组排序：
		      * PARTITION BY :指定窗口函数分组的Key；
		      * ORDER BY：分组后进行排序；
		      */
		 
        String sqlText = "SELECT UserID,Item, count "
		         + "FROM ("
		           + "SELECT "
		             + "UserID,Item, count,"
		             + "row_number() OVER (PARTITION BY UserID ORDER BY count DESC) rank"
		             +" FROM userItemInformations "
		         + ") sub_userItemInformations "
		         + "WHERE rank <= 3 " ;
       
            System.out.println(sqlText);
            
			DataFrame execellentNameAgeDF = sqlContext.sql(sqlText);
			execellentNameAgeDF.show(); 
		//	execellentNameAgeDF.write().format("json").save("G://IMFBigDataSpark2016//tesdata//SparkSQLUserlogsHottest//Result20140419.json");
				
	}

}
