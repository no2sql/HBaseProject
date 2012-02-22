import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcConnection {



	static String userid = "system";
	static String password = "research";
	static String url = "jdbc:oracle:thin:@10.50.5.29:1521:CSNRDEE";
	static String schema = "schaturvedi_711";
	static String jdbcDriver = "oracle.jdbc.driver.OracleDriver";
    static int rows = 1000;


	public static ResultSet getData(Connection conn, String query) {
		ResultSet rs = null;
		try {
			Statement statement = conn.createStatement();
			statement.setFetchSize(rows);
			rs = statement.executeQuery(query);
			statement.close();
		} catch(SQLException ex) {
		
			System.err.println("SQLException:" + ex.getMessage());
		}
		return rs;
	}


	public static Connection getOracleJDBCConnection(){
		Connection con = null ;
		try {
			Class.forName(jdbcDriver);
			con = DriverManager.getConnection(url, userid, password);
			Statement stm = con.createStatement();
			//stm.execute("ALTER SESSION SET CURRENT_SCHEMA =" +schema);
			stm.close();
		} catch(java.lang.ClassNotFoundException e) {
			//System.err.println(“ClassNotFoundException: “);
			System.err.println(e.getMessage());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
/*
		try {
			con = DriverManager.getConnection(url, userid, password);
		} catch(SQLException ex) {
			//System.err.println(“SQLException: ” + ex.getMessage());
		}
*/
		return con;
	}

}
