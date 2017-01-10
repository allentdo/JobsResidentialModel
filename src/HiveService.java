import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveService {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String ip="x.x.x.x";
	private static int port=10000;
	private static String sql=null;
	 private static ResultSet res;  
	
	public Connection getConnection()
	{
		Connection connection = null;
		try {
			try {
				Class.forName(driverName);
			
			connection = DriverManager.getConnection("jdbc:hive://"
					+ ip + ":" + port + "/zhizhumoxing", "", "");
			System.out.println("Get connection succesed.");
		} catch (SQLException e) {
			System.out.println("Get connection failed.");
			
		}
			} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return connection;
		
	}
	
	
	  static void countData(Statement stmt, String tableName) throws SQLException{
	        sql = "select count(1) from " + tableName;  
	        System.out.println("Running:" + sql);  
	        res = stmt.executeQuery(sql);  
	        System.out.println("执行“regular hive query”运行结果:");  
	        while (res.next()) {  
	            System.out.println("count ------>" + res.getString(1));  
	        }  
	    }  
	  
	     static void selectData(Statement stmt, String tableName)  
	            throws SQLException {  
	        sql = "select * from " + tableName;  
	        System.out.println("Running:" + sql);  
	        res = stmt.executeQuery(sql);  
	        System.out.println("执行 select * query 运行结果:");  
	        while (res.next()) {  
	            System.out.println(res.getInt(1) + "\t" + res.getString(2));  
	        }  
	    }  
	  
	    static void loadData(Statement stmt, String tableName)  
	            throws SQLException {  
	        String filepath = "/home/test.txt";  
	        sql = "load data local inpath '" + filepath + "' into table "  
	                + tableName;  
	        System.out.println("Running:" + sql);  
	        res = stmt.executeQuery(sql);  
	    }  
	  
	    static void describeTables(Statement stmt, String tableName)  
	            throws SQLException {  
	        sql = "describe " + tableName;  
	        System.out.println("Running:" + sql);  
	        res = stmt.executeQuery(sql);  
	        System.out.println("执行 describe table 运行结果:");  
	        while (res.next()) {  
	            System.out.println(res.getString(1) + "\t" + res.getString(2));  
	        }  
	    }  
	  
	    static void showTables(Statement stmt, String tableName)  
	            throws SQLException {  
	        sql = "show tables '" + tableName + "'";  
	        System.out.println("Running:" + sql);  
	        res = stmt.executeQuery(sql);  
	        System.out.println("执行 show tables 运行结果:");  
	        if (res.next()) {  
	            System.out.println(res.getString(1));  
	        }  
	    }  
	
	  void createTable(Statement stmt, String tableName)  
	            throws SQLException {  
	    String sql = "create table "  
	                + tableName  
	                + " (key int, value string)  row format delimited fields terminated by '\t'";  
	    stmt.execute("drop table if exists " + tableName);
	        stmt.executeQuery(sql);  
	        System.out.println("Create table success!");
	    }  
	  
	    static String dropTable(Statement stmt,String tableName) throws SQLException {  
	        // 创建的表名  
	        String sql = "drop table " + tableName;  
	        stmt.executeQuery(sql);  
	        System.out.println("drop table success!");
	        return tableName;  
	    }  
	  
	
	
	
	/**
	 * 查询操作
	 * 
	 * @param sql
	 * @param st
	 * @throws SQLException
	 */
	public ResultSet select(String sql, Statement st) throws SQLException {
		return st.executeQuery(sql);
	}

}
