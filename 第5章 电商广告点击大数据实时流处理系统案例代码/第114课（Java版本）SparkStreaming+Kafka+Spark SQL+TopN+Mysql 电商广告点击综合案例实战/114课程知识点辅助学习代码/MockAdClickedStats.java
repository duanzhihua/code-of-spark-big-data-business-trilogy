package com.dt.spark.IMF114;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MockAdClickedStats {

	public static void main(String[] args) {
		final Random random = new Random();
		final String[] provinces = new String[]{"Guangdong","Zhejiang","Jiangsu","Fujian"};
		final Map<String,String[]> cities = new HashMap<String, String[]>();
		cities.put("Guangdong", new String[]{"Guangzhou","Shenzhen","DongGuan"});
		cities.put("Zhejiang", new String[]{"Hangzhou","Wenzhou","Ningbo"});
		cities.put("Jiangsu", new String[]{"Nanjing","Suzhou","WuXi"});
		cities.put("Fujian", new String[]{"Fuzhou","Ximen","Sanming"});
		
		final String[] ips = new String[]{
				"192.168.112.240",
				"192.168.112.239",
				"192.168.112.245",
				"192.168.112.246",
				"192.168.112.247",
				"192.168.112.248",
				"192.168.112.249",
				"192.168.112.250",
				"192.168.112.251",
				"192.168.112.252",
				"192.168.112.253",
				"192.168.112.254"
		};
		/**
		 * Kafka相关的基本配置信息
		 */
		Properties kafkaConf = new Properties();
		kafkaConf.put("serializer.class", "kafka.serializer.StringEncoder");
		kafkaConf.put("metadata.broker.list", "master:9092,worker1:9092,worker2:9092");
		ProducerConfig producerConfig = new ProducerConfig(kafkaConf);
		
		final Producer<Integer, String> producer = new Producer<Integer, String>(producerConfig);
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(true){
					//在线处理广告点击流 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
					Long timestamp = new Date().getTime();
					String ip = ips[random.nextInt(12)]; //可以采用网络上免费提供的IP库
					int userID = random.nextInt(10000);
					int adID = random.nextInt(100);
					String province = provinces[random.nextInt(4)];
					String city = cities.get(province)[random.nextInt(3)];
					
					String clickedAd = timestamp + "\t" + ip + "\t" + userID + "\t" + adID + "\t" + province + 
							"\t" + city ;
					
					System.out.println(clickedAd);
					
					producer.send( new KeyedMessage("AdClicked",clickedAd));
					
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}			
			}
		}).start();
		
		
	}

}
