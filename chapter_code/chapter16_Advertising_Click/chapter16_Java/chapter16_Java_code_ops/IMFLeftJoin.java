package com.dt.spark.IMF114;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Optional;

import scala.Tuple2;

public class IMFLeftJoin {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLwithJoin");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<Integer, Integer>> alists = new ArrayList<Tuple2<Integer, Integer>>();
		alists.add(new Tuple2<Integer, Integer>(666, 100666));
		alists.add(new Tuple2<Integer, Integer>(888, 100888));
		alists.add(new Tuple2<Integer, Integer>(999, 100999));
		JavaPairRDD<Integer, Integer>  a =  sc.parallelizePairs(alists);
		 
		List<Tuple2<Integer, Integer>> blists = new ArrayList<Tuple2<Integer, Integer>>();
		blists.add(new Tuple2<Integer, Integer>(666, 200666));
		blists.add(new Tuple2<Integer, Integer>(999, 200999));
		JavaPairRDD<Integer, Integer> b=  sc.parallelizePairs(blists);
		
	    JavaPairRDD<Integer, Tuple2<Integer, Integer>> resultRDD=a.join(b);
	    
	  
	    
	    List<Tuple2<Integer, Tuple2<Integer, Integer>>>   results = resultRDD.collect();
			for(  Tuple2<Integer, Tuple2<Integer, Integer>> row : results){
				System.out.println(row._1  + " "+  row._2._1 + " "+  row._2._2);
			}

		 JavaPairRDD<Integer,Tuple2<Integer,Optional<Integer>>> leftResultRDD=a.leftOuterJoin(b);
		 List<Tuple2<Integer, Tuple2<Integer, Optional<Integer>>>> leftResults= leftResultRDD.collect();
		 
		 for( Tuple2<Integer, Tuple2<Integer, Optional<Integer>>> leftrow : leftResults ){
			 
			  Optional<Integer> optional = leftrow._2._2;
			    if (optional.isPresent()  ) {
					// System.out.println( optional.get());
					 System.out.println(leftrow._1  + " "+  leftrow._2._1 + " "+  leftrow._2._2);
				} else {
					 System.out.println( leftrow._1  + " "+  leftrow._2._1 + " "+ "leftrow._2._2：null");
				}

			 
				
			}
		 
	}

}
