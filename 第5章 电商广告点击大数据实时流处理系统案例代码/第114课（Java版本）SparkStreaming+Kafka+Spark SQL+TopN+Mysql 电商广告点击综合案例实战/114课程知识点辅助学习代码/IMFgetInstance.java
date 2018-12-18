package com.dt.spark.IMF114;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.LinkedBlockingQueue;

 

public class IMFgetInstance {

	public static void main(String[] args) {
		JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
		// new JDBCWrapper () ;
		 
	}

}
class JDBCWrapper {

	private static JDBCWrapper jdbcInstance = null;
	private static LinkedBlockingQueue<Connection> dbConnectionPool = new LinkedBlockingQueue<Connection>();

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static JDBCWrapper getJDBCInstance() {
		if (jdbcInstance == null) {

			synchronized (JDBCWrapper.class) {
				if (jdbcInstance == null) {
					jdbcInstance = new JDBCWrapper();
				}
			}

		}

		return jdbcInstance;
	}

	private JDBCWrapper() {

		for (int i = 0; i < 10; i++) {

			try {
				Connection conn = DriverManager.getConnection("jdbc:mysql://Master:3306/sparkstreaming", "root",
						"root");
				dbConnectionPool.put(conn);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
}