package com.dt.spark.SparkApps.sql;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Random;

/**
 * 电商数据自动生成代码，数据格式如下：
 *  用户数据 ：{"userID": 0, "name": "spark0", "registeredTime": "2016-10-11 18:06:25"}
 *  日志数据：{"logID": 00,"userID": 0, "time": "2016-10-04 15:42:45", "typed": 0, "consumed": 0.0}
 */
public class Mock_EB_Users_Data
{

	public static void main(String[] args ) throws ParseException {
		/**
		 * 通过传递进来的参数生成指定大小规模的数据；
		 */
		String dataPath = "data/Mock_EB_Users_Data/";
	 	long numberItems = 100;
		mockUserData(numberItems,dataPath);
		mockLogData(numberItems,dataPath);

	}

	private static void mockLogData(long numberItems, String dataPath) {
      //{"logID": 00,"userID": 0, "time": "2016-10-04 15:42:45", "typed": 0, "consumed": 0.0}
		StringBuffer mock_Log_Buffer = new StringBuffer("");
		Random random = new Random();
	 	for(int i = 0; i <numberItems;i++) { //userID
			for(int j = 0; j <numberItems;j++) {
				String initData = "2016-10-";
				String randomData = String.format("%s%02d%s%02d%s%02d%s%02d", initData, random.nextInt(31)
						, " ", random.nextInt(24)
						, ":", random.nextInt(60)
						, ":", random.nextInt(60));
				String	result = "{\"logID\": " + String.format("%02d",j) + ", \"userID\": " + i + ", \"time\": \"" + randomData + "\", \"" +
						"typed\": " +String.format("%01d",random.nextInt(2))+
						", \"consumed\":" +String.format("%.2f",random.nextDouble()*1000)
						+"}";

				mock_Log_Buffer.append(result)
						.append("\n");

			}
		 }
		System.out.println(mock_Log_Buffer);
		PrintWriter printWriter = null;
		try {
			printWriter = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(dataPath + "Mock_EB_Log_Data.log")));
			printWriter.write(mock_Log_Buffer.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			printWriter.close();
		}


	}

	private static void mockUserData(long numberItems, String dataPath) {
		StringBuffer mock_User_Buffer = new StringBuffer("");
		Random random = new Random();
		for(int i = 0; i <numberItems;i++) {
			String initData = "2016-10-";
			String randomData = String.format("%s%02d%s%02d%s%02d%s%02d", initData, random.nextInt(31)
					, " ", random.nextInt(24)
					, ":", random.nextInt(60)
					, ":", random.nextInt(60));
		 	String	result = "{\"userID\": " + i + ", \"name\": \"spark" + i + "\", \"registeredTime\": \"" + randomData + "\"}";
			mock_User_Buffer.append(result)
					.append("\n");

		}
		System.out.println(mock_User_Buffer);
		PrintWriter printWriter = null;
		try {
			printWriter = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(dataPath + "Mock_EB_Users_Data.log")));
			printWriter.write(mock_User_Buffer.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			printWriter.close();
		}
	}
}
